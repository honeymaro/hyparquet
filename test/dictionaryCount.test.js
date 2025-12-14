import { describe, expect, it } from 'vitest'
import { asyncBufferFromFile } from '../src/node.js'
import { parquetReadDictionary, parquetReadDictionaryCount } from '../src/read.js'

describe('parquetReadDictionaryCount', () => {
  it('should return count matching actual dictionary length', async () => {
    const file = await asyncBufferFromFile('test/files/issue23.parquet')

    const count = await parquetReadDictionaryCount({
      file,
      columns: ['menu.categories.dishes.name.value'],
    })

    const dictionary = await parquetReadDictionary({
      file,
      columns: ['menu.categories.dishes.name.value'],
    })

    expect(count).toBeDefined()
    expect(typeof count).toBe('number')
    expect(count).toBe(dictionary?.length)
  })

  it('should work with integer dictionary encoded columns', async () => {
    const file = await asyncBufferFromFile('test/files/signs.parquet')

    const count = await parquetReadDictionaryCount({
      file,
      columns: ['unsigned_int8'],
    })

    const dictionary = await parquetReadDictionary({
      file,
      columns: ['unsigned_int8'],
    })

    if (dictionary) {
      expect(count).toBe(dictionary.length)
    }
  })

  it('should work with string dictionary columns', async () => {
    const file = await asyncBufferFromFile('test/files/plain-dict-uncompressed-checksum.parquet')

    const count = await parquetReadDictionaryCount({
      file,
      columns: ['binary_field'],
    })

    const dictionary = await parquetReadDictionary({
      file,
      columns: ['binary_field'],
    })

    if (dictionary) {
      expect(count).toBe(dictionary.length)
    }
  })

  it('should return undefined for non-dictionary column', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')

    const count = await parquetReadDictionaryCount({
      file,
      columns: ['a'],
    })

    expect(count).toBeUndefined()
  })

  it('should throw error for multiple columns', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')

    await expect(parquetReadDictionaryCount({
      file,
      columns: ['a', 'b'],
    })).rejects.toThrow('parquetReadDictionaryCount expected columns: [columnName]')
  })

  it('should throw error for non-existent column', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')

    await expect(parquetReadDictionaryCount({
      file,
      columns: ['nonexistent'],
    })).rejects.toThrow("Column 'nonexistent' not found")
  })
})
