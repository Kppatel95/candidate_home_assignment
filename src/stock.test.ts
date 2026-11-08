import assert from 'node:assert/strict'
import test from 'node:test'
import { applyPayload, parsePayload } from './stock.js'

test('snapshot replaces the complete stock', () => {
  const parsed = parsePayload('snapshot', { stock: { milk: 8 } })
  assert.notEqual(typeof parsed, 'string')
  if (typeof parsed === 'string') throw new Error(parsed)

  assert.deepEqual(applyPayload({ eggs: 3 }, parsed), { milk: 8 })
})

test('delta removes unset keys and applies set values', () => {
  const parsed = parsePayload('delta', {
    set: { milk: 12, bread: 2 },
    unset: ['eggs'],
  })
  assert.notEqual(typeof parsed, 'string')
  if (typeof parsed === 'string') throw new Error(parsed)

  assert.deepEqual(
    applyPayload({ milk: 10, eggs: 6 }, parsed),
    { milk: 12, bread: 2 },
  )
})

test('delta unset wins if a key appears in both set and unset', () => {
  const parsed = parsePayload('delta', { set: { milk: 12 }, unset: ['milk'] })
  assert.notEqual(typeof parsed, 'string')
  if (typeof parsed === 'string') throw new Error(parsed)

  assert.deepEqual(applyPayload({ milk: 10 }, parsed), {})
})

test('malformed payloads return a clear validation error', () => {
  assert.equal(parsePayload('snapshot', { stock: [] }), 'snapshot payload must contain a stock object')
  assert.equal(parsePayload('delta', { set: {}, unset: [3] }), 'delta payload must contain an unset array of strings')
})
