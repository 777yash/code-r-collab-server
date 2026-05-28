import 'dotenv/config'
import http from 'http'
import { WebSocket, WebSocketServer } from 'ws'
import * as Y from 'yjs'

// y-websocket CJS server utilities
const {
  setupWSConnection,
  setPersistence,
  setContentInitializor,
  // eslint-disable-next-line @typescript-eslint/no-var-requires
} = require('y-websocket/bin/utils') as {
  setupWSConnection: (ws: WebSocket, req: http.IncomingMessage, opts?: { docName?: string; gc?: boolean }) => void
  setPersistence: (p: {
    bindState: (docName: string, doc: Y.Doc) => Promise<void>
    writeState: (docName: string, doc: Y.Doc) => Promise<void>
    provider: null
  }) => void
  setContentInitializor: (f: (doc: Y.Doc) => Promise<void>) => void
}

import { loadSnapshot, saveSnapshot, saveAutoSnapshot } from './snapshot.js'
import { initRedis, wireDocPubSub } from './redis-pubsub.js'

initRedis()

const PORT = Number(process.env.PORT ?? 1234)
const SNAPSHOT_INTERVAL_MS = 60_000
// Render free tier idles after ~15 min. Heartbeat keeps live connections open;
// for zero-client wake-ups an external pinger (UptimeRobot / Render cron) is needed.
const WS_HEARTBEAT_MS = 25_000

// Attach snapshot save interval + Redis pub/sub to each new doc
// y-websocket sets doc.name at runtime but it's not in Y.Doc types
setContentInitializor(async (doc: Y.Doc) => {
  const docName = (doc as Y.Doc & { name: string }).name

  const timer = setInterval(() => {
    saveSnapshot(docName, doc)
    saveAutoSnapshot(docName, doc)
  }, SNAPSHOT_INTERVAL_MS)

  doc.on('destroy', () => clearInterval(timer))

  wireDocPubSub(docName, doc)
})

// Wire persistence: load on first join, save on last leave
setPersistence({
  provider: null,
  bindState: async (docName: string, doc: Y.Doc) => {
    const snapshot = await loadSnapshot(docName)
    if (snapshot) {
      Y.applyUpdate(doc, snapshot)
      console.log(`[persistence] loaded snapshot for room "${docName}" (${snapshot.byteLength}b)`)
    } else {
      console.log(`[persistence] no snapshot for room "${docName}", starting fresh`)
    }
  },
  writeState: async (docName: string, doc: Y.Doc) => {
    await saveSnapshot(docName, doc)
    console.log(`[persistence] saved snapshot for room "${docName}" on last client leave`)
  },
})

const server = http.createServer((_req, res) => {
  res.writeHead(200)
  res.end('collab-server ok')
})

const wss = new WebSocketServer({ server })

type AliveWS = WebSocket & { isAlive: boolean }

const heartbeat = setInterval(() => {
  wss.clients.forEach((client) => {
    const ws = client as AliveWS
    if (!ws.isAlive) {
      ws.terminate()
      return
    }
    ws.isAlive = false
    ws.ping()
  })
}, WS_HEARTBEAT_MS)

wss.on('close', () => clearInterval(heartbeat))

wss.on('connection', (ws: WebSocket, req: http.IncomingMessage) => {
  const alive = ws as AliveWS
  alive.isAlive = true
  alive.on('pong', () => { alive.isAlive = true })

  // URL: ws://host:1234/<roomId>
  const roomId = (req.url ?? '/').slice(1).split('?')[0]
  console.log(`[ws] client connected → room "${roomId}"`)
  setupWSConnection(ws, req, { docName: roomId })
})

server.listen(PORT, () => {
  console.log(`[collab-server] listening on http/ws://localhost:${PORT}`)
})

process.on('SIGTERM', () => {
  console.log('[collab-server] shutting down...')
  server.close(() => process.exit(0))
})
