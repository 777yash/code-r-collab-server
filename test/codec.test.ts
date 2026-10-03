import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as Y from 'yjs'
import { gzipSync } from 'node:zlib'
import {
  decodeInto,
  decodeSnapshotBase64,
  encodeSnapshot,
  readSnapshot,
  validateSnapshot,
  SnapshotValidationError,
  MAX_SNAPSHOT_BYTES,
  MAX_EXPANDED_SNAPSHOT_BYTES,
} from '../src/codec.js'

function workspace() {
  const doc = new Y.Doc()
  doc
    .getMap('file-list')
    .set(
      'default',
      JSON.stringify({
        id: 'default',
        name: 'src/main.js',
        language: 'javascript',
        order: 0,
      })
    )
  doc.getText('file:default').insert(0, 'const answer = 42')
  return doc
}
function tagged(payload: Uint8Array, flags: number) {
  return Buffer.concat([Buffer.from([0x59, 0x5a, flags]), Buffer.from(payload)])
}
test('reads legacy V1 and every supported tagged format', () => {
  const doc = workspace()
  const v1 = Y.encodeStateAsUpdate(doc),
    v2 = Y.encodeStateAsUpdateV2(doc)
  for (const bytes of [
    v1,
    tagged(v1, 0),
    tagged(v2, 1),
    tagged(gzipSync(v1), 2),
    encodeSnapshot(doc),
  ]) {
    const decoded = readSnapshot(bytes)
    assert.equal(
      decoded.getText('file:default').toString(),
      'const answer = 42'
    )
    decoded.destroy()
  }
  doc.destroy()
})
test('accepts empty documents and legacy single-file content', () => {
  const doc = new Y.Doc()
  validateSnapshot(Y.encodeStateAsUpdate(doc))
  doc.getText('content').insert(0, 'legacy')
  const decoded = readSnapshot(encodeSnapshot(doc))
  assert.equal(decoded.getText('content').toString(), 'legacy')
  decoded.destroy()
  doc.destroy()
})
test('rejects malformed headers, gzip, binary updates and trailing bytes', () => {
  const doc = workspace()
  for (const bytes of [
    Buffer.alloc(0),
    Buffer.from([0xff]),
    Buffer.from([0x59, 0x5a]),
    tagged(Buffer.from([0, 0]), 4),
    tagged(Buffer.from('bad gzip'), 3),
    Buffer.concat([Y.encodeStateAsUpdate(doc), Buffer.from([0])]),
    tagged(Buffer.concat([Y.encodeStateAsUpdateV2(doc), Buffer.from([0])]), 1),
  ]) {
    assert.throws(() => validateSnapshot(bytes), SnapshotValidationError)
  }
  doc.destroy()
})
test('rejects differential updates with missing dependencies', () => {
  const doc = new Y.Doc()
  doc.getText('content').insert(0, 'before')
  const vector = Y.encodeStateVector(doc)
  doc.getText('content').insert(6, 'after')
  assert.throws(
    () => validateSnapshot(Y.encodeStateAsUpdate(doc, vector)),
    SnapshotValidationError
  )
  doc.destroy()
})
test('rejects invalid metadata, wrong root shapes and embedded file content before mutating a target', () => {
  const target = workspace()
  const before = Y.encodeStateAsUpdate(target)
  const invalidDocs: Y.Doc[] = []
  for (const raw of [
    '{',
    'null',
    JSON.stringify({ id: 'other', name: 'a', language: 'js', order: 0 }),
    JSON.stringify({
      id: 'default',
      name: '../secret',
      language: 'js',
      order: 0,
    }),
  ]) {
    const doc = workspace()
    doc.getMap('file-list').set('default', raw)
    invalidDocs.push(doc)
  }
  const array = new Y.Doc()
  array.getArray('file-list').push(['bad'])
  invalidDocs.push(array)
  const map = new Y.Doc()
  map.getMap('file:default').set('bad', 'value')
  invalidDocs.push(map)
  const textArray = new Y.Doc()
  textArray.getArray('content').push(['silently discarded'])
  invalidDocs.push(textArray)
  const embed = workspace()
  embed.getText('file:default').insertEmbed(0, { malicious: true })
  invalidDocs.push(embed)
  const subdoc = workspace()
  subdoc.getMap('file-list').set('subdoc', new Y.Doc())
  invalidDocs.push(subdoc)
  const result = workspace()
  result.getMap('execution-results').set('latest', '{"stdout":{},"stderr":""}')
  invalidDocs.push(result)
  for (const doc of invalidDocs) {
    assert.throws(
      () => decodeInto(target, Y.encodeStateAsUpdate(doc)),
      SnapshotValidationError
    )
    assert.deepEqual(Y.encodeStateAsUpdate(target), before)
    doc.destroy()
  }
  target.destroy()
})
test('accepts legitimate collaboration metadata', () => {
  const doc = workspace()
  doc.getMap('ai-control').set('request', true)
  doc
    .getMap('execution-results')
    .set('latest', JSON.stringify({ stdout: 'ok', stderr: '', exitCode: 0 }))
  doc
    .getArray('chat-messages')
    .push([
      {
        id: 'm',
        userId: 'ai',
        userName: 'AI',
        content: 'hello',
        timestamp: 1,
        ai: {
          status: 'done',
          triggeredBy: 'user',
          reqId: 'req',
          files: ['src/main.js'],
        },
      },
    ])
  validateSnapshot(encodeSnapshot(doc))
  doc.destroy()
})
test('strictly validates base64 without silently accepting empty, URL-safe or noncanonical data', () => {
  for (const value of [
    undefined,
    null,
    123,
    '',
    '%%%%',
    'AAA',
    'AA A=',
    'AB==',
    '____',
    'AAAA=',
  ]) {
    assert.throws(() => decodeSnapshotBase64(value), SnapshotValidationError)
  }
  assert.deepEqual(decodeSnapshotBase64('AAA='), Buffer.from([0, 0]))
})
test('bounds stored, base64 and decompressed snapshot sizes', () => {
  const tooLarge = (error: unknown) =>
    error instanceof SnapshotValidationError && error.status === 413
  assert.throws(
    () => validateSnapshot(Buffer.alloc(MAX_SNAPSHOT_BYTES + 1)),
    tooLarge
  )
  assert.throws(
    () =>
      decodeSnapshotBase64(
        'A'.repeat(4 * Math.ceil(MAX_SNAPSHOT_BYTES / 3) + 4)
      ),
    tooLarge
  )
  const bomb = tagged(
    gzipSync(Buffer.alloc(MAX_EXPANDED_SNAPSHOT_BYTES + 1)),
    3
  )
  assert.throws(() => validateSnapshot(bomb), tooLarge)
  const doc = new Y.Doc()
  doc.getText('content').insert(0, 'x'.repeat(MAX_EXPANDED_SNAPSHOT_BYTES + 1))
  assert.throws(() => encodeSnapshot(doc), tooLarge)
  doc.destroy()
})
