import { EventEmitter } from 'node:events'
import assert from 'node:assert/strict'
import test from 'node:test'
import * as Y from 'yjs'
import { Awareness, removeAwarenessStates } from 'y-protocols/awareness'
import { createRoomMesh } from '../src/room-mesh'
import { createServer } from 'node:http'
import { WebSocket } from 'ws'
import { WebsocketProvider } from 'y-websocket'
import { attachSecureWebSocket } from '../src/secure-websocket'
import Redis from 'ioredis'
import { config as loadEnv } from 'dotenv'
import { randomUUID } from 'node:crypto'
import type { MeshTransport } from '../src/room-mesh'

const {
  setupWSConnection,
  setContentInitializor,
  setPersistence,
  docs: serverDocs,
} = require('y-websocket/bin/utils')

class Transport extends EventEmitter {
  channels = new Set<string>()
  connected = true
  constructor(readonly peers: Set<Transport>) {
    super()
    peers.add(this)
  }
  async subscribe(channel: string) {
    this.channels.add(channel)
  }
  async unsubscribe(channel: string) {
    this.channels.delete(channel)
  }
  async publish(channel: string, message: string) {
    if (!this.connected) return
    for (const peer of this.peers)
      if (peer.connected && peer.channels.has(channel)) {
        queueMicrotask(() => peer.emit('message', channel, message))
      }
  }
}
async function settle() {
  for (let i = 0; i < 8; i++)
    await new Promise<void>((resolve) => setImmediate(resolve))
}
function fixture() {
  const peers = new Set<Transport>()
  const transports = [new Transport(peers), new Transport(peers)]
  const meshes = transports.map((transport) =>
    createRoomMesh(transport, transport)
  )
  const docs = [new Y.Doc(), new Y.Doc()]
  const awareness = docs.map((doc) => new Awareness(doc))
  awareness.forEach((a) => a.setLocalState(null))
  return {
    transports,
    meshes,
    docs,
    awareness,
    close: () => {
      awareness.forEach((a) => a.destroy())
      docs.forEach((d) => d.destroy())
      meshes.forEach((m) => m.close())
    },
  }
}

test('a late server receives existing room content and both servers converge without echoes', async () => {
  const f = fixture()
  try {
    f.docs[0].getText('code').insert(0, 'existing code')
    f.meshes[0].wire('room', f.docs[0], f.awareness[0])
    await settle()
    f.meshes[1].wire('room', f.docs[1], f.awareness[1])
    await settle()
    assert.equal(f.docs[1].getText('code').toString(), 'existing code')
    f.docs[0].getText('code').insert(0, 'A ')
    f.docs[1].getText('code').insert(0, 'B ')
    await settle()
    assert.equal(
      f.docs[0].getText('code').toString(),
      f.docs[1].getText('code').toString()
    )
    assert.match(f.docs[0].getText('code').toString(), /existing code/)
  } finally {
    f.close()
  }
})
test('cursor awareness crosses servers and removals reach peers', async () => {
  const f = fixture()
  try {
    f.awareness[0].setLocalState({
      user: { id: 'owner' },
      cursor: { line: 2 },
    })
    f.meshes[0].wire('room', f.docs[0], f.awareness[0])
    await settle()
    f.meshes[1].wire('room', f.docs[1], f.awareness[1])
    await settle()
    assert.equal(
      f.awareness[1].getStates().get(f.docs[0].clientID)?.user.id,
      'owner'
    )
    f.awareness[0].setLocalStateField('cursor', { line: 9 })
    await settle()
    assert.equal(
      f.awareness[1].getStates().get(f.docs[0].clientID)?.cursor.line,
      9
    )
    removeAwarenessStates(f.awareness[0], [f.docs[0].clientID], 'disconnect')
    await settle()
    assert.equal(f.awareness[1].getStates().has(f.docs[0].clientID), false)
  } finally {
    f.close()
  }
})
test('reconnecting exchanges edits made on both sides during a transport outage', async () => {
  const f = fixture()
  try {
    f.meshes.forEach((mesh, i) => mesh.wire('room', f.docs[i]))
    await settle()
    f.transports[1].connected = false
    f.docs[0].getText('code').insert(0, 'online')
    f.docs[1].getText('code').insert(0, 'offline')
    await settle()
    f.transports[1].connected = true
    f.transports[1].emit('ready')
    await settle()
    const content = f.docs[0].getText('code').toString()
    assert.equal(content, f.docs[1].getText('code').toString())
    assert.match(content, /online/)
    assert.match(content, /offline/)
  } finally {
    f.close()
  }
})
test('rooms stay isolated, malformed messages are contained, and destroyed rooms unsubscribe', async () => {
  const f = fixture()
  try {
    f.meshes[0].wire('one', f.docs[0])
    f.meshes[1].wire('two', f.docs[1])
    await settle()
    f.docs[0].getText('code').insert(0, 'private')
    await settle()
    assert.equal(f.docs[1].getText('code').toString(), '')
    for (const raw of [
      '{',
      'null',
      JSON.stringify({ id: 'peer', kind: 'update', update: '!!!' }),
      JSON.stringify({ id: 'peer', kind: 'update', update: 'AAAA' }),
    ]) {
      f.transports[0].emit('message', 'ydoc:one', raw)
    }
    assert.equal(f.docs[0].getText('code').toString(), 'private')
    f.docs[0].destroy()
    assert.equal(f.transports[0].channels.size, 0)
  } finally {
    f.close()
  }
})

async function verifyTwoServers(f: {
  meshes: ReturnType<typeof createRoomMesh>[]
  close: () => void
}) {
  const channelName = `qa-mesh-${randomUUID()}`
  const ownerDoc = new Y.Doc()
  const viewerDoc = new Y.Doc()
  const servers = [createServer(), createServer()]
  const providers: WebsocketProvider[] = []
  const services: ReturnType<typeof attachSecureWebSocket>[] = []
  setPersistence(null)
  setContentInitializor(
    async (doc: Y.Doc & { name: string; awareness: Awareness }) => {
      const index = doc.name === 'mesh-server-0' ? 0 : 1
      f.meshes[index].wire(channelName, doc, doc.awareness)
    }
  )
  const waitUntil = async (predicate: () => boolean) => {
    const deadline = Date.now() + 3000
    while (!predicate()) {
      if (Date.now() > deadline) throw new Error('WebSocket sync timed out')
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
  }
  try {
    for (let index = 0; index < 2; index++) {
      services.push(
        attachSecureWebSocket(servers[index], {
          allowedOrigin: 'https://app.example',
          authorize: async (roomId, ticket) =>
            roomId === 'room'
              ? {
                  roomId,
                  userId: ticket === 'owner' ? 'owner-id' : 'viewer-id',
                  name: ticket === 'owner' ? 'Owner' : 'Viewer',
                  role: ticket === 'owner' ? 'OWNER' : 'VIEWER',
                  clientId:
                    ticket === 'owner' ? ownerDoc.clientID : viewerDoc.clientID,
                  expiresAt: Date.now() + 60_000,
                }
              : null,
          setup: (ws, req) =>
            setupWSConnection(ws, req, { docName: `mesh-server-${index}` }),
        })
      )
      await new Promise<void>((resolve) =>
        servers[index].listen(0, '127.0.0.1', resolve)
      )
    }
    class OriginWebSocket extends WebSocket {
      constructor(url: string) {
        super(url, { origin: 'https://app.example' })
      }
    }
    const connect = (index: number, ticket: string, doc: Y.Doc) => {
      const port = (servers[index].address() as { port: number }).port
      const provider = new WebsocketProvider(
        `ws://127.0.0.1:${port}`,
        'room',
        doc,
        {
          params: { ticket },
          disableBc: true,
          WebSocketPolyfill: OriginWebSocket as never,
        }
      )
      providers.push(provider)
      return provider
    }
    const owner = connect(0, 'owner', ownerDoc)
    await waitUntil(() => owner.synced)
    ownerDoc.getText('content').insert(0, 'before second server')
    await waitUntil(
      () => serverDocs.get('mesh-server-0')?.getText('content').length > 0
    )
    const viewer = connect(1, 'viewer', viewerDoc)
    await waitUntil(
      () =>
        viewer.synced &&
        viewerDoc.getText('content').toString() === 'before second server'
    )
    ownerDoc.getArray('chat').push([{ content: 'shared chat' }])
    owner.awareness.setLocalState({
      user: { id: 'spoofed', name: 'Spoofed' },
      cursor: { line: 4 },
    })
    await waitUntil(
      () =>
        viewerDoc.getArray('chat').length === 1 &&
        viewer.awareness.getStates().get(ownerDoc.clientID)?.user.id ===
          'owner-id'
    )
    viewerDoc.getText('content').insert(0, 'viewer injection')
    await settle()
    assert.equal(ownerDoc.getText('content').toString(), 'before second server')
    assert.equal(
      serverDocs.get('mesh-server-1').getText('content').toString(),
      'before second server'
    )
    owner.disconnect()
    await waitUntil(() => !viewer.awareness.getStates().has(ownerDoc.clientID))
  } finally {
    providers.forEach((p) => p.destroy())
    services.forEach((s) => s.close())
    await Promise.all(
      servers.map(
        (s) => new Promise<void>((resolve) => s.close(() => resolve()))
      )
    )
    for (const doc of serverDocs.values()) doc.destroy()
    serverDocs.clear()
    setContentInitializor(async () => {})
    ownerDoc.destroy()
    viewerDoc.destroy()
    f.close()
  }
}

test('actual WebSocket clients on two servers sync edits, chat and authenticated cursor identity', async () => {
  await verifyTwoServers(fixture())
})

test(
  'live Redis transports carry collaboration between two WebSocket servers',
  {
    skip: process.env.RUN_REDIS_INTEGRATION !== '1',
  },
  async () => {
    loadEnv()
    const url = process.env.UPSTASH_REDIS_URL || process.env.REDIS_URL
    assert.ok(url, 'UPSTASH_REDIS_URL or REDIS_URL required')
    const clients: Redis[] = []
    const meshes: ReturnType<typeof createRoomMesh>[] = []
    try {
      for (let i = 0; i < 2; i++) {
        const options = {
          lazyConnect: true,
          connectTimeout: 5000,
          retryStrategy: () => null,
          maxRetriesPerRequest: 0,
        }
        const pub = new Redis(url, options)
        const sub = new Redis(url, options)
        clients.push(pub, sub)
        for (const client of [pub, sub]) client.on('error', () => {})
        await Promise.all([pub.connect(), sub.connect()])
        meshes.push(createRoomMesh(pub, sub as unknown as MeshTransport))
      }
      await verifyTwoServers({
        meshes,
        close: () => {
          meshes.forEach((mesh) => mesh.close())
          clients.forEach((client) => client.disconnect())
        },
      })
    } finally {
      meshes.forEach((mesh) => mesh.close())
      clients.forEach((client) => client.disconnect())
    }
  }
)
