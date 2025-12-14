import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { parquetPlan, prefetchAsyncBuffer } from './plan.js'
import { assembleAsync, asyncGroupToRows, readRowGroup } from './rowgroup.js'
import { readColumnDictionary, readColumnDictionaryCount } from './column.js'
import { getSchemaPath } from './schema.js'
import { getColumnRange } from './plan.js'
import { DEFAULT_PARSERS } from './convert.js'
import { concat, flatten } from './utils.js'

/**
 * @import {AsyncRowGroup, DecodedArray, ParquetReadOptions, BaseParquetReadOptions, ParquetDictionaryOptions, ParquetDictionaryCountOptions} from '../src/types.js'
 */
/**
 * Read parquet data rows from a file-like object.
 * Reads the minimal number of row groups and columns to satisfy the request.
 *
 * Returns a void promise when complete.
 * Errors are thrown on the returned promise.
 * Data is returned in callbacks onComplete, onChunk, onPage, NOT the return promise.
 * See parquetReadObjects for a more convenient API.
 *
 * @param {ParquetReadOptions} options read options
 * @returns {Promise<void>} resolves when all requested rows and columns are parsed, all errors are thrown here
 */
export async function parquetRead(options) {
  // load metadata if not provided
  options.metadata ??= await parquetMetadataAsync(options.file, options)

  // read row groups
  const asyncGroups = parquetReadAsync(options)

  const { rowStart = 0, rowEnd, columns, onChunk, onComplete, rowFormat } = options

  // skip assembly if no onComplete or onChunk, but wait for reading to finish
  if (!onComplete && !onChunk) {
    for (const { asyncColumns } of asyncGroups) {
      for (const { data } of asyncColumns) await data
    }
    return
  }

  // assemble struct columns
  const schemaTree = parquetSchema(options.metadata)
  const assembled = asyncGroups.map(arg => assembleAsync(arg, schemaTree))

  // onChunk emit all chunks (don't await)
  if (onChunk) {
    for (const asyncGroup of assembled) {
      for (const asyncColumn of asyncGroup.asyncColumns) {
        asyncColumn.data.then(columnDatas => {
          let rowStart = asyncGroup.groupStart
          for (const columnData of columnDatas) {
            onChunk({
              columnName: asyncColumn.pathInSchema[0],
              columnData,
              rowStart,
              rowEnd: rowStart + columnData.length,
            })
            rowStart += columnData.length
          }
        })
      }
    }
  }

  // onComplete transpose column chunks to rows
  if (onComplete) {
    // loosen the types to avoid duplicate code
    /** @type {any[]} */
    const rows = []
    for (const asyncGroup of assembled) {
      // filter to rows in range
      const selectStart = Math.max(rowStart - asyncGroup.groupStart, 0)
      const selectEnd = Math.min((rowEnd ?? Infinity) - asyncGroup.groupStart, asyncGroup.groupRows)
      // transpose column chunks to rows in output
      const groupData = rowFormat === 'object' ?
        await asyncGroupToRows(asyncGroup, selectStart, selectEnd, columns, 'object') :
        await asyncGroupToRows(asyncGroup, selectStart, selectEnd, columns, 'array')
      concat(rows, groupData)
    }
    onComplete(rows)
  } else {
    // wait for all async groups to finish (complete takes care of this)
    for (const { asyncColumns } of assembled) {
      for (const { data } of asyncColumns) await data
    }
  }
}

/**
 * @param {ParquetReadOptions} options read options
 * @returns {AsyncRowGroup[]}
 */
export function parquetReadAsync(options) {
  if (!options.metadata) throw new Error('parquet requires metadata')
  // TODO: validate options (start, end, columns, etc)

  // prefetch byte ranges
  const plan = parquetPlan(options)
  options.file = prefetchAsyncBuffer(options.file, plan)

  // read row groups
  return plan.groups.map(groupPlan => readRowGroup(options, plan, groupPlan))
}

/**
 * Reads a single column from a parquet file.
 *
 * @param {BaseParquetReadOptions} options
 * @returns {Promise<DecodedArray>}
 */
export async function parquetReadColumn(options) {
  if (options.columns?.length !== 1) {
    throw new Error('parquetReadColumn expected columns: [columnName]')
  }
  options.metadata ??= await parquetMetadataAsync(options.file, options)
  const asyncGroups = parquetReadAsync(options)

  // assemble struct columns
  const schemaTree = parquetSchema(options.metadata)
  const assembled = asyncGroups.map(arg => assembleAsync(arg, schemaTree))

  /** @type {DecodedArray[]} */
  const columnData = []
  for (const rg of assembled) {
    columnData.push(flatten(await rg.asyncColumns[0].data))
  }
  return flatten(columnData)
}

/**
 * This is a helper function to read parquet row data as a promise.
 * It is a wrapper around the more configurable parquetRead function.
 *
 * @param {Omit<ParquetReadOptions, 'onComplete'>} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
 */
export function parquetReadObjects(options) {
  return new Promise((onComplete, reject) => {
    parquetRead({
      ...options,
      rowFormat: 'object', // force object output
      onComplete,
    }).catch(reject)
  })
}

/**
 * Extracts categorical column dictionary/categories from a parquet file.
 * Useful for getting unique values of categorical columns.
 *
 * @param {ParquetDictionaryOptions} options read options (must specify single column)
 * @returns {Promise<DecodedArray | undefined>} dictionary values for the categorical column, or undefined if not categorical
 */
export async function parquetReadDictionary(options) {
  if (!options.columns || options.columns.length !== 1) {
    throw new Error('parquetReadDictionary expected columns: [columnName]')
  }

  const columnName = options.columns[0]
  options.metadata ??= await parquetMetadataAsync(options.file, options)

  let columnFound = false

  // Search all row groups for dictionary data
  for (const rowGroup of options.metadata.row_groups) {
    for (const columnChunk of rowGroup.columns) {
      const { meta_data } = columnChunk
      if (!meta_data) continue

      // Check if this is the column we're looking for (support nested columns)
      const columnPath = meta_data.path_in_schema.join('.')
      if (columnPath === columnName) {
        columnFound = true

        // Check if this row group has a dictionary page
        if (meta_data.dictionary_page_offset !== undefined) {
          const { startByte, endByte } = getColumnRange(meta_data)
          const arrayBuffer = await options.file.slice(startByte, endByte)
          const schemaPath = getSchemaPath(options.metadata.schema, meta_data.path_in_schema)
          const reader = { view: new DataView(arrayBuffer), offset: 0 }

          const columnDecoder = {
            columnName: meta_data.path_in_schema.join('.'),
            type: meta_data.type,
            element: schemaPath[schemaPath.length - 1].element,
            schemaPath,
            codec: meta_data.codec,
            parsers: { ...DEFAULT_PARSERS, ...options.parsers },
            compressors: options.compressors,
            utf8: options.utf8 ?? true,
          }

          return readColumnDictionary(reader, columnDecoder)
        }
        // No dictionary in this row group, continue searching other row groups
      }
    }
  }

  if (!columnFound) {
    throw new Error(`Column '${columnName}' not found`)
  }

  return undefined // Column exists but has no dictionary encoding in any row group
}

/**
 * Get dictionary count (cardinality) for a categorical column.
 * Much faster than parquetReadDictionary as it only reads the page header,
 * without decompressing or decoding the dictionary data.
 *
 * @param {ParquetDictionaryCountOptions} options read options (must specify single column)
 * @returns {Promise<number | undefined>} dictionary count, or undefined if not dictionary encoded
 */
export async function parquetReadDictionaryCount(options) {
  if (!options.columns || options.columns.length !== 1) {
    throw new Error('parquetReadDictionaryCount expected columns: [columnName]')
  }

  const columnName = options.columns[0]
  options.metadata ??= await parquetMetadataAsync(options.file, options)

  let columnFound = false

  // Search row groups for dictionary page
  for (const rowGroup of options.metadata.row_groups) {
    for (const columnChunk of rowGroup.columns) {
      const { meta_data } = columnChunk
      if (!meta_data) continue

      const columnPath = meta_data.path_in_schema.join('.')
      if (columnPath === columnName) {
        columnFound = true

        if (meta_data.dictionary_page_offset !== undefined) {
          // Optimization: only read enough bytes for the page header (~256 bytes)
          const startByte = Number(meta_data.dictionary_page_offset)
          const endByte = startByte + 256

          const arrayBuffer = await options.file.slice(startByte, endByte)
          const reader = { view: new DataView(arrayBuffer), offset: 0 }

          return readColumnDictionaryCount(reader)
        }
      }
    }
  }

  if (!columnFound) {
    throw new Error(`Column '${columnName}' not found`)
  }

  return undefined
}
