import { expect, it } from 'vitest'
import { parquetReadColumn, parquetReadDictionary } from '../src/read.js'
import { asyncBufferFromFile } from '../src/node.js'

it('should return dictionary indices when rawDictionary is true', async () => {
  // Using a file that has dictionary encoded columns
  const file = await asyncBufferFromFile('test/files/plain-dict-uncompressed-checksum.parquet')
  
  // First, get the dictionary to understand the mapping
  const dictionary = await parquetReadDictionary({
    file,
    columns: ['binary_field'], // BYTE_ARRAY column with PLAIN_DICTIONARY encoding
  })
  
  if (dictionary) {
    console.log('Dictionary values:', Array.from(dictionary))
    
    // Read the column with dictionary decoding (default behavior)
    const decodedColumn = await parquetReadColumn({
      file,
      columns: ['binary_field'],
      rawDictionary: false, // explicit false for clarity
    })
    
    // Read the column with raw dictionary indices
    const rawIndicesColumn = await parquetReadColumn({
      file,
      columns: ['binary_field'],
      rawDictionary: true,
    })
    
    console.log('First 10 decoded values:', decodedColumn.slice(0, 10))
    console.log('First 10 raw indices:', rawIndicesColumn.slice(0, 10))
    
    // Verify that we can reconstruct decoded values from indices
    for (let i = 0; i < Math.min(10, decodedColumn.length); i++) {
      const expectedValue = dictionary[rawIndicesColumn[i]]
      expect(decodedColumn[i]).toBe(expectedValue)
    }
    
    // Raw indices should be integers
    expect(rawIndicesColumn.every(val => Number.isInteger(val))).toBe(true)
    
    // Dictionary lookup should produce the same results as decoded column
    const reconstructed = Array.from(rawIndicesColumn).map(index => dictionary[index])
    expect(reconstructed).toEqual(Array.from(decodedColumn))
  } else {
    console.log('Column "binary_field" is not dictionary encoded, skipping test')
  }
})

it('should work with integer dictionary encoded columns', async () => {
  // Using signs.parquet which has integer dictionary encoded columns
  const file = await asyncBufferFromFile('test/files/signs.parquet')
  
  const dictionary = await parquetReadDictionary({
    file,
    columns: ['unsigned_int8'],
  })
  
  if (dictionary) {
    console.log('Integer dictionary:', Array.from(dictionary))
    
    const decoded = await parquetReadColumn({
      file,
      columns: ['unsigned_int8'],
      rawDictionary: false,
    })
    
    const rawIndices = await parquetReadColumn({
      file,
      columns: ['unsigned_int8'],
      rawDictionary: true,
    })
    
    console.log('Decoded values:', Array.from(decoded))
    console.log('Raw indices:', Array.from(rawIndices))
    
    // Verify indices map correctly to decoded values
    for (let i = 0; i < decoded.length; i++) {
      expect(decoded[i]).toBe(dictionary[rawIndices[i]])
    }
    
    expect(rawIndices.every(val => Number.isInteger(val))).toBe(true)
  }
})
