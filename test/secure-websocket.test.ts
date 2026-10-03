import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { once } from 'node:events'
import { WebSocket } from 'ws'
import * as Y from 'yjs'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'
import { attachSecureWebSocket } from '../src/secure-websocket'
import { createAuthorizer, type Access } from '../src/auth'

const { setupWSConnection, docs, setPersistence } = require('y-websocket/bin/utils')
setPersistence(null)

async function eventually(check: () => boolean) {
  const until = Date.now() + 2000
  while (!check()) {
    assert.ok(Date.now() < until, 'condition did not become true')
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

async function fixture() {
  const server = createServer()
  const access = new Map<string, Access>()
  let initializations = 0
  let hang = false
  const service = attachSecureWebSocket(server, {
    allowedOrigin: 'https://app.example', checkIntervalMs: 25, leaseMs: 100,
    authorize: async (room, token) => {
      if (hang) return new Promise(() => {})
      const found = room === 'room' ? access.get(token) : null
      return found ? { ...found } : null
    },
    setup: (socket, req, room) => { initializations++; setupWSConnection(socket, req, { docName: room }) },
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address() as { port: number }
  const base = `ws://127.0.0.1:${address.port}`
  const ticket = (name: string, role: Access['role'], clientId: number, expiresAt = Date.now() + 60_000) => {
    access.set(name, { userId: name, name: `${name} name`, role, clientId, expiresAt })
    return name
  }
  const connect = async (token: string) => {
    const ws = new WebSocket(`${base}/room?ticket=${token}`, { origin: 'https://app.example' })
    const messages: Uint8Array[] = []
    ws.on('message', (data) => messages.push(new Uint8Array(data as Buffer)))
    ws.on('error', () => {})
    await once(ws, 'open')
    return { ws, messages }
  }
  const reject = async (path: string, origin = 'https://app.example') => {
    const ws = new WebSocket(`${base}${path}`, { origin })
    let data = 0
    ws.on('message', () => data++)
    ws.on('error', () => {})
    const status = await new Promise<number>((resolve, reject) => {
      ws.on('open', () => reject(new Error('Unauthorized connection opened')))
      ws.on('unexpected-response', (_req, res) => { resolve(res.statusCode!); res.resume(); ws.terminate() })
    })
    assert.equal(data, 0)
    return status
  }
  return { access, service, ticket, connect, reject, get initializations() { return initializations },
    hang: () => { hang = true },
    close: async () => {
      service.close()
      await new Promise<void>((resolve) => server.close(() => resolve()))
      for (const doc of docs.values()) doc.destroy()
      docs.clear()
    },
  }
}

function sync(subtype: number, data: Uint8Array) {
  const encoder = encoding.createEncoder()
  encoding.writeVarUint(encoder, 0)
  encoding.writeVarUint(encoder, subtype)
  encoding.writeVarUint8Array(encoder, data)
  return encoding.toUint8Array(encoder)
}

async function readDocument(client: { ws: WebSocket; messages: Uint8Array[] }) {
  const start = client.messages.length
  const empty = new Y.Doc()
  client.ws.send(sync(0, Y.encodeStateVector(empty)))
  const isReply = (bytes: Uint8Array) => {
    const decoder = decoding.createDecoder(bytes)
    return decoding.readVarUint(decoder) === 0 && decoding.readVarUint(decoder) === 1
  }
  await eventually(() => client.messages.slice(start).some(isReply))
  const decoder = decoding.createDecoder(client.messages.slice(start).find(isReply)!)
  decoding.readVarUint(decoder); decoding.readVarUint(decoder)
  Y.applyUpdate(empty, decoding.readVarUint8Array(decoder))
  const content = empty.getText('content').toString()
  empty.destroy()
  return content
}

test('rejects anonymous, wrong-room, expired and cross-origin connections before creating a document', async () => {
  const f = await fixture()
  try {
    f.ticket('editor', 'EDITOR', 1)
    f.ticket('expired', 'OWNER', 2, Date.now() - 1)
    assert.equal(await f.reject('/room'), 401)
    assert.equal(await f.reject('/room?ticket=invalid'), 403)
    assert.equal(await f.reject('/other?ticket=editor'), 403)
    assert.equal(await f.reject('/room?ticket=expired'), 403)
    assert.equal(await f.reject('/room?ticket=editor', 'https://evil.example'), 401)
    assert.equal(f.initializations, 0)
    assert.equal(docs.size, 0)
  } finally { await f.close() }
})

test('editors write; viewers read but cannot write via update OR syncStep2', async () => {
  const f = await fixture()
  const editorDoc = new Y.Doc()
  const hostileDoc = new Y.Doc()
  try {
    const editor = await f.connect(f.ticket('editor', 'EDITOR', editorDoc.clientID))
    editorDoc.getText('content').insert(0, 'private code')
    editor.ws.send(sync(2, Y.encodeStateAsUpdate(editorDoc)))
    assert.equal(await readDocument(editor), 'private code')
    const viewer = await f.connect(f.ticket('viewer', 'VIEWER', hostileDoc.clientID))
    assert.equal(await readDocument(viewer), 'private code')
    hostileDoc.getText('content').insert(0, 'injected')
    viewer.ws.send(sync(2, Y.encodeStateAsUpdate(hostileDoc)))
    viewer.ws.send(sync(1, Y.encodeStateAsUpdate(hostileDoc)))
    assert.equal(await readDocument(viewer), 'private code')
    assert.equal(docs.get('room').getText('content').toString(), 'private code')
  } finally { editorDoc.destroy(); hostileDoc.destroy(); await f.close() }
})

test('membership revocation and role changes disconnect idle sockets and are checked on reconnect', async () => {
  const f = await fixture()
  try {
    const editor = await f.connect(f.ticket('editor', 'EDITOR', 1))
    const downgraded = await f.connect(f.ticket('downgraded', 'EDITOR', 2))
    const editorClosed = once(editor.ws, 'close')
    const downgradedClosed = once(downgraded.ws, 'close')
    f.access.delete('editor')
    f.access.get('downgraded')!.role = 'VIEWER'
    const [code] = await editorClosed
    assert.equal(code, 4403)
    assert.equal(await f.reject('/room?ticket=editor'), 403)
    assert.equal((await downgradedClosed)[0], 4403)
    const viewer = await f.connect('downgraded')
    const viewerClosed = once(viewer.ws, 'close')
    f.service.revoke('room')
    assert.equal((await viewerClosed)[0], 4403)
  } finally { await f.close() }
})

test('expired tickets and stalled authorization stop idle connections', async () => {
  const f = await fixture()
  try {
    const expiring = await f.connect(f.ticket('short', 'VIEWER', 1, Date.now() + 70))
    const closed = once(expiring.ws, 'close')
    assert.equal((await closed)[0], 4401)
    const client = await f.connect(f.ticket('viewer', 'VIEWER', 2))
    f.hang()
    assert.equal((await once(client.ws, 'close'))[0], 4401)
  } finally { await f.close() }
})

test('presence identity is server-owned and cannot overwrite another client', async () => {
  const f = await fixture()
  try {
    const client = await f.connect(f.ticket('alice', 'EDITOR', 42))
    assert.equal(await f.reject('/room?ticket=alice'), 409)
    const awareness = encoding.createEncoder()
    encoding.writeVarUint(awareness, 2)
    for (const id of [42, 99]) {
      encoding.writeVarUint(awareness, id)
      encoding.writeVarUint(awareness, 1)
      encoding.writeVarString(awareness, JSON.stringify({ user: { id: 'owner', name: 'spoofed' } }))
    }
    const encoder = encoding.createEncoder()
    encoding.writeVarUint(encoder, 1)
    encoding.writeVarUint8Array(encoder, encoding.toUint8Array(awareness))
    client.ws.send(encoding.toUint8Array(encoder))
    await readDocument(client)
    const states = docs.get('room').awareness.getStates()
    assert.equal(states.get(42).user.id, 'alice')
    assert.equal(states.get(42).user.name, 'alice name')
    assert.equal(states.has(99), false)
  } finally { await f.close() }
})

test('authorizer requires a secret and fails closed on API errors or malformed responses', async () => {
  assert.throws(() => createAuthorizer('http://localhost', ''))
  const originalFetch = globalThis.fetch
  const authorize = createAuthorizer('http://localhost', 'a'.repeat(32))
  try {
    globalThis.fetch = async () => new Response('{}', { status: 500 })
    assert.equal(await authorize('room', 'token'), null)
    globalThis.fetch = async () => new Response('{}')
    assert.equal(await authorize('room', 'token'), null)
    globalThis.fetch = async () => { throw new Error('offline') }
    assert.equal(await authorize('room', 'token'), null)
  } finally { globalThis.fetch = originalFetch }
})
