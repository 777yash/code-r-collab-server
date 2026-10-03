import { test, mock } from 'node:test'
import assert from 'node:assert/strict'
import { createServer, request } from 'node:http'
import { once } from 'node:events'
import { WebSocket } from 'ws'
import * as Y from 'yjs'
import { createResetHandler } from '../src/snapshot-reset.js'
import {
  createSnapshotLoader,
  restoreLiveSnapshot,
} from '../src/snapshot-state.js'
import {
  encodeSnapshot,
  MAX_SNAPSHOT_BYTES,
  MAX_SNAPSHOT_JSON_BYTES,
  SnapshotValidationError,
} from '../src/codec.js'
import {
  loadSnapshot,
  saveSnapshot,
  saveAutoSnapshot,
} from '../src/snapshot.js'
import { attachSecureWebSocket } from '../src/secure-websocket.js'

function workspace(text = 'original') {
  const doc = new Y.Doc()
  doc
    .getMap('file-list')
    .set(
      'default',
      JSON.stringify({
        id: 'default',
        name: 'main.js',
        language: 'javascript',
        order: 0,
      })
    )
  doc.getText('file:default').insert(0, text)
  return doc
}
test('HTTP reset rejects invalid input, preserves live state, and remains available for valid requests', async () => {
  const doc = workspace()
  const docs = new Map([['room', doc]])
  const reset = createResetHandler(
    (req) => req.headers['x-internal-secret'] === 'secret',
    docs
  )
  const server = createServer((req, res) => {
    void reset(req, res, req.url === '/missing' ? 'missing' : 'room')
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const url = `http://127.0.0.1:${(server.address() as { port: number }).port}`
  const before = Y.encodeStateAsUpdate(doc)
  try {
    for (const body of [
      '{',
      '{}',
      'null',
      '{"data":123}',
      '{"data":"%%%"}',
      '{"data":"////"}',
    ]) {
      for (const path of ['/room', '/missing']) {
        const res = await fetch(url + path, {
          method: 'POST',
          headers: { 'x-internal-secret': 'secret' },
          body,
        })
        assert.equal(res.status, 400)
        await res.arrayBuffer()
        assert.deepEqual(Y.encodeStateAsUpdate(doc), before)
      }
    }
    const unauthorized = await fetch(url, { method: 'POST', body: '{}' })
    assert.equal(unauthorized.status, 401)
    await unauthorized.arrayBuffer()
    // Real chunked HTTP request: rejection must not destroy the response socket.
    const status = await new Promise<number>((resolve, reject) => {
      const req = request(
        url,
        { method: 'POST', headers: { 'x-internal-secret': 'secret' } },
        (res) => {
          res.resume()
          res.on('end', () => resolve(res.statusCode!))
        }
      )
      req.on('error', reject)
      req.write(Buffer.alloc(MAX_SNAPSHOT_JSON_BYTES))
      req.end(Buffer.from('x'))
    })
    assert.equal(status, 413)
    assert.deepEqual(Y.encodeStateAsUpdate(doc), before)
    const restored = workspace('restored')
    const body = JSON.stringify({
      data: encodeSnapshot(restored).toString('base64'),
    })
    restored.destroy()
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'x-internal-secret': 'secret' },
      body,
    })
    assert.equal(res.status, 200)
    assert.deepEqual(await res.json(), { applied: true })
    assert.equal(doc.getText('file:default').toString(), 'restored')
  } finally {
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
    doc.destroy()
  }
})
test('restore preflights all files before deleting or updating live data', () => {
  const live = workspace()
  const bad = new Y.Doc()
  bad
    .getMap('file-list')
    .set(
      'new',
      JSON.stringify({
        id: 'new',
        name: 'new.js',
        language: 'javascript',
        order: 0,
      })
    )
  bad.getMap('file:new').set('wrong type', 'value')
  const before = Y.encodeStateAsUpdate(live)
  assert.throws(
    () => restoreLiveSnapshot(live, Y.encodeStateAsUpdate(bad)),
    SnapshotValidationError
  )
  assert.deepEqual(Y.encodeStateAsUpdate(live), before)
  bad.destroy()
  live.destroy()
})
test('restores empty legacy content', () => {
  const live = new Y.Doc(),
    restored = new Y.Doc()
  live.getText('content').insert(0, 'clear this')
  restored.getText('content').insert(0, 'deleted')
  restored.getText('content').delete(0, 7)
  restoreLiveSnapshot(live, encodeSnapshot(restored))
  assert.equal(live.getText('content').toString(), '')
  live.destroy()
  restored.destroy()
})
test('corrupt startup load resolves safely without mutating or allowing persistence', async () => {
  const doc = workspace()
  const before = Y.encodeStateAsUpdate(doc)
  let failed = 0
  const loader = createSnapshotLoader(
    async () => Buffer.from('invalid'),
    (_room, failedDoc) => {
      failed++
      assert.equal(loader.canSave(failedDoc), false)
    }
  )
  await loader.bindState('room', doc)
  assert.equal(failed, 1)
  assert.equal(loader.canSave(doc), false)
  assert.deepEqual(Y.encodeStateAsUpdate(doc), before)
  doc.destroy()
})
test('valid startup load still merges state', async () => {
  const doc = new Y.Doc(),
    source = workspace()
  const loader = createSnapshotLoader(
    async () => encodeSnapshot(source),
    () => assert.fail('Unexpected failure')
  )
  await loader.bindState('room', doc)
  assert.equal(loader.canSave(doc), true)
  assert.equal(doc.getText('file:default').toString(), 'original')
  doc.destroy()
  source.destroy()
})
test('corrupt stored snapshot closes actual sockets without triggering a last-leave overwrite', async () => {
  const {
    setupWSConnection,
    setPersistence,
    docs,
  } = require('y-websocket/bin/utils')
  let saved = 0,
    failed = 0
  const loader = createSnapshotLoader(
    async () => {
      // Yield until setup attaches the connection.
      await new Promise((resolve) => setTimeout(resolve, 20))
      return Buffer.from('invalid')
    },
    (_room, doc) => {
      failed++
      for (const conn of Array.from(
        (doc as Y.Doc & { conns: Map<WebSocket, unknown> }).conns.keys()
      )) {
        conn.close(1011, 'Stored snapshot is invalid')
      }
    }
  )
  setPersistence({
    provider: null,
    bindState: loader.bindState,
    writeState: async (_room: string, doc: Y.Doc) => {
      if (loader.canSave(doc)) saved++
    },
  })
  const server = createServer()
  const service = attachSecureWebSocket(server, {
    allowedOrigin: 'https://app.example',
    authorize: async () => ({
      userId: 'user',
      name: 'User',
      role: 'EDITOR',
      clientId: 1,
      expiresAt: Date.now() + 60_000,
    }),
    setup: (socket, req, room) =>
      setupWSConnection(socket, req, { docName: room }),
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const ws = new WebSocket(
    `ws://127.0.0.1:${(server.address() as { port: number }).port}/room?ticket=valid`,
    { origin: 'https://app.example' }
  )
  ws.on('error', () => {})
  try {
    const [code] = await once(ws, 'close')
    assert.equal(code, 1011)
    assert.equal(failed, 1)
    assert.equal(saved, 0)
  } finally {
    service.close()
    await new Promise<void>((resolve) => server.close(() => resolve()))
    for (const doc of docs.values()) doc.destroy()
    docs.clear()
    setPersistence(null)
  }
})
test('bounds snapshot HTTP loads and distinguishes an empty successful body from 204', async () => {
  const fetchMock = mock.method(globalThis, 'fetch')
  try {
    fetchMock.mock.mockImplementation(
      async () => new Response(null, { status: 204 })
    )
    assert.equal(await loadSnapshot('room'), null)
    fetchMock.mock.mockImplementation(
      async () => new Response(new Uint8Array(MAX_SNAPSHOT_BYTES + 1))
    )
    await assert.rejects(
      loadSnapshot('room'),
      (error) =>
        error instanceof SnapshotValidationError && error.status === 413
    )
    fetchMock.mock.mockImplementation(
      async () => new Response(new Uint8Array(0))
    )
    let failed = false
    const doc = new Y.Doc()
    const loader = createSnapshotLoader(loadSnapshot, () => {
      failed = true
    })
    await loader.bindState('room', doc)
    assert.equal(failed, true)
    assert.equal(loader.canSave(doc), false)
    doc.destroy()
  } finally {
    fetchMock.mock.restore()
  }
})
test('invalid save encoding is contained before upload in both save helpers', async () => {
  const fetchMock = mock.method(
    globalThis,
    'fetch',
    async () => new Response(null, { status: 204 })
  )
  const logMock = mock.method(console, 'error', () => {})
  const doc = workspace()
  doc.getMap('file-list').set('default', 'invalid')
  try {
    await saveSnapshot('room', doc)
    await saveAutoSnapshot('room', doc)
    assert.equal(fetchMock.mock.callCount(), 0)
    assert.equal(logMock.mock.callCount(), 2)
  } finally {
    fetchMock.mock.restore()
    logMock.mock.restore()
    doc.destroy()
  }
})

test('save helpers report failed HTTP uploads and bound request duration', async () => {
  const fetchMock = mock.method(globalThis, 'fetch', async (_url: unknown, options: RequestInit) => {
    assert.ok(options.signal instanceof AbortSignal)
    return new Response(null, { status: 503 })
  })
  const doc = workspace()
  try {
    assert.equal(await saveSnapshot('room', doc), false)
    assert.equal(await saveAutoSnapshot('room', doc), false)
    fetchMock.mock.mockImplementation(async () => new Response(null, { status: 204 }))
    assert.equal(await saveSnapshot('room', doc), true)
    assert.equal(await saveAutoSnapshot('room', doc), true)
  } finally {
    fetchMock.mock.restore()
    doc.destroy()
  }
})
