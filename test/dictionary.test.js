import { describe, expect, it } from 'vitest'
import { asyncBufferFromFile } from '../src/node.js'
import { parquetReadDictionary } from '../src/read.js'

describe('parquetReadDictionary', () => {
  it('should extract dictionary from categorical column with dictionary encoding', async () => {
    const file = await asyncBufferFromFile('test/files/issue23.parquet')
    
    // Try with a column that has dictionary encoding
    const dictionary = await parquetReadDictionary({
      file,
      columns: ['menu.categories.dishes.name.value'],
    })
    
    expect(dictionary).toBeDefined()
    if (dictionary) {
      expect(Array.isArray(dictionary) || dictionary instanceof Uint8Array).toBe(true)
      expect(dictionary.length).toBeGreaterThan(0)
      
      // Should contain actual string values
      const categories = Array.from(dictionary)
      expect(categories).toContain('Test')
    }
  })

  it('should return undefined for non-categorical column', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    
    const dictionary = await parquetReadDictionary({
      file,
      columns: ['a'], // column without dictionary encoding
    })
    
    expect(dictionary).toBeUndefined()
  })

  it('should throw error for multiple columns', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    
    await expect(parquetReadDictionary({
      file,
      columns: ['a', 'b'],
    })).rejects.toThrow('parquetReadDictionary expected columns: [columnName]')
  })

  it('should throw error for non-existent column', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    
    await expect(parquetReadDictionary({
      file,
      columns: ['nonexistent'],
    })).rejects.toThrow("Column 'nonexistent' not found")
  })
})
