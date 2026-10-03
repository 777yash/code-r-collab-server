import 'dotenv/config'
import http from 'http'
import { timingSafeEqual } from 'node:crypto'
import { createAuthorizer } from './auth.js'
import {
  attachSecureWebSocket,
  type AuthorizedSocket,
} from './secure-websocket.js'
import * as Y from 'yjs'

// y-websocket CJS server utilities
const {
  setupWSConnection,
  setPersistence,
  setContentInitializor,
  docs,
  // eslint-disable-next-line @typescript-eslint/no-var-requires
} = require('y-websocket/bin/utils') as {
  setupWSConnection: (
    ws: AuthorizedSocket,
    req: http.IncomingMessage,
    opts?: { docName?: string; gc?: boolean }
  ) => void
  setPersistence: (p: {
    bindState: (docName: string, doc: Y.Doc) => Promise<void>
    writeState: (docName: string, doc: Y.Doc) => Promise<void>
    provider: null
  }) => void
  setContentInitializor: (f: (doc: Y.Doc) => Promise<void>) => void
  docs: Map<string, Y.Doc>
}

import { loadSnapshot, saveSnapshot, saveAutoSnapshot } from './snapshot.js'
import { createSnapshotLoader } from './snapshot-state.js'
import { createResetHandler } from './snapshot-reset.js'
import { readBody } from './http-body.js'
import { scheduleSnapshots } from './snapshot-scheduler.js'
import { initRedis, wireDocPubSub, closeRedis } from './redis-pubsub.js'

const internalSecret = process.env.NEXTJS_INTERNAL_SECRET ?? ''
const apiUrl = process.env.NEXTJS_API_URL ?? 'http://localhost:3000'
const authorize = createAuthorizer(apiUrl, internalSecret)
const allowedOrigin = new URL(process.env.COLLAB_ALLOWED_ORIGIN ?? apiUrl)
  .origin

function isInternal(req: http.IncomingMessage): boolean {
  const supplied = Buffer.from(String(req.headers['x-internal-secret'] ?? ''))
  const expected = Buffer.from(internalSecret)
  return (
    supplied.length === expected.length && timingSafeEqual(supplied, expected)
  )
}

initRedis()

const PORT = Number(process.env.PORT ?? 1234)
const SNAPSHOT_INTERVAL_MS = 30_000

const snapshotLoader = createSnapshotLoader(
  loadSnapshot,
  (roomId, doc, error) => {
    console.error(
      `[persistence] invalid snapshot for room "${roomId}"; refusing to save:`,
      error
    )
    const conns = (doc as Y.Doc & { conns: Map<AuthorizedSocket, unknown> })
      .conns
    for (const conn of Array.from(conns.keys())) {
      try {
        conn.close(1011, 'Stored snapshot is invalid')
      } catch (closeError) {
        console.error('[persistence] socket close failed:', closeError)
      }
    }
  }
)
const handleResetDoc = createResetHandler(isInternal, docs)

// Attach snapshot save interval + Redis pub/sub to each new doc
// y-websocket sets doc.name at runtime but it's not in Y.Doc types
setContentInitializor(async (doc: Y.Doc) => {
  const docName = (doc as Y.Doc & { name: string }).name

  // Delete-only updates do not advance a Yjs state vector. Track updates instead.
  scheduleSnapshots(
    doc,
    () => snapshotLoader.canSave(doc),
    async () => {
      const results = await Promise.all([
        saveSnapshot(docName, doc),
        saveAutoSnapshot(docName, doc),
      ])
      return results.every(Boolean)
    },
    SNAPSHOT_INTERVAL_MS
  )

  wireDocPubSub(docName, doc)
})

// Wire persistence: load on first join, save on last leave
setPersistence({
  provider: null,
  bindState: snapshotLoader.bindState,
  writeState: async (docName: string, doc: Y.Doc) => {
    if (!snapshotLoader.canSave(doc)) return
    if (!(await saveSnapshot(docName, doc))) return
    console.log(
      `[persistence] saved snapshot for room "${docName}" on last client leave`
    )
  },
})

async function handleHttpRequest(
  req: http.IncomingMessage,
  res: http.ServerResponse
): Promise<void> {
  const url = req.url ?? '/'

  if (req.method === 'POST' && url.startsWith('/revoke-access/')) {
    if (!isInternal(req)) {
      res.writeHead(401)
      res.end()
      return
    }
    try {
      const roomId = decodeURIComponent(
        url.slice('/revoke-access/'.length).split('?')[0]
      )
      const body = JSON.parse((await readBody(req, 4096)).toString())
      if (body.userId !== undefined && typeof body.userId !== 'string')
        throw new Error('Invalid user')
      collaboration.revoke(roomId, body.userId)
      res.writeHead(204)
    } catch {
      res.writeHead(400)
    }
    res.end()
    return
  }

  if (req.method === 'POST' && url.startsWith('/reset-doc/')) {
    const roomId = url.slice('/reset-doc/'.length).split('?')[0]
    await handleResetDoc(req, res, roomId)
    return
  }

  res.writeHead(200)
  res.end('collab-server ok')
}

const server = http.createServer((req, res) => {
  void handleHttpRequest(req, res).catch((error) => {
    console.error('[http] request failed:', error)
    if (!res.headersSent) res.writeHead(500)
    res.end()
  })
})

const collaboration = attachSecureWebSocket(server, {
  authorize,
  allowedOrigin,
  setup: (ws, req, roomId) => setupWSConnection(ws, req, { docName: roomId }),
})

server.listen(PORT, () => {
  console.log(`[collab-server] listening on http/ws://localhost:${PORT}`)
})

process.on('SIGTERM', () => {
  console.log('[collab-server] shutting down...')
  collaboration.close()
  closeRedis()
  server.close(() => process.exit(0))
})
