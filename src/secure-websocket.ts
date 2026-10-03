import { EventEmitter } from 'node:events'
import type { IncomingMessage, Server } from 'node:http'
import type { Duplex } from 'node:stream'
import { WebSocket, WebSocketServer, type RawData } from 'ws'
import * as decoding from 'lib0/decoding'
import * as encoding from 'lib0/encoding'
import type { Access } from './auth.js'

const CHECK_INTERVAL_MS = 5000
const ACCESS_LEASE_MS = 10_000

function color(id: string): string {
  let hash = 0
  for (const ch of id) hash = (hash * 31 + ch.charCodeAt(0)) >>> 0
  return `hsl(${hash % 360}, 80%, 60%)`
}

// The Yjs utility sees only this socket. Both inbound mutations and outbound
// broadcasts pass through the access lease, including initial syncStep2 writes.
export class AuthorizedSocket extends EventEmitter {
  binaryType = 'arraybuffer'
  private leaseUntil: number
  private ended = false
  constructor(readonly socket: WebSocket, readonly access: Access, leaseMs: number) {
    super()
    this.leaseUntil = Math.min(Date.now() + leaseMs, access.expiresAt)
    socket.on('pong', () => this.emit('pong'))
    socket.on('error', () => this.close())
    socket.on('close', () => this.finish())
    socket.on('message', (data: RawData, binary: boolean) => {
      if (!this.allowed()) return
      if (!binary) return this.close(1003, 'Binary protocol required')
      try {
        const bytes = Buffer.isBuffer(data) ? data : data instanceof ArrayBuffer ? Buffer.from(data) : Buffer.concat(data)
        const decoder = decoding.createDecoder(bytes)
        const type = decoding.readVarUint(decoder)
        if (type === 0) {
          const subtype = decoding.readVarUint(decoder)
          if (subtype > 2) return this.close(1008, 'Invalid sync message')
          // Viewers still exchange state vectors; their state is never applied.
          if (this.access.role === 'VIEWER' && subtype !== 0) return
          this.emit('message', bytes)
        } else if (type === 1) {
          this.emit('message', this.ownAwareness(decoding.readVarUint8Array(decoder)))
        } else if (type === 3) {
          // Awareness queries are not needed: setupWSConnection sends it on join.
        } else {
          this.close(1008, 'Unknown protocol message')
        }
      } catch {
        this.close(1008, 'Invalid protocol message')
      }
    })
  }
  get readyState() { return this.ended ? WebSocket.CLOSED : this.socket.readyState }
  renew(leaseMs: number) { this.leaseUntil = Math.min(Date.now() + leaseMs, this.access.expiresAt) }
  allowed(): boolean {
    if (this.ended) return false
    if (Date.now() >= this.leaseUntil) {
      this.close(4401, 'Authorization expired')
      return false
    }
    return true
  }
  send(data: Uint8Array, _options: unknown, callback: (error?: Error) => void) {
    if (!this.allowed()) { callback(new Error('Authorization expired')); return }
    this.socket.send(data, callback)
  }
  ping() { if (this.allowed()) this.socket.ping() }
  close(code = 1000, reason = '') {
    if (this.ended) return
    // Remove from Yjs immediately; do not wait for an untrusted peer's close ack.
    this.finish()
    this.socket.close(code, reason)
  }
  private finish() {
    if (this.ended) return
    this.ended = true
    this.emit('close')
  }
  private ownAwareness(update: Uint8Array): Uint8Array {
    const decoder = decoding.createDecoder(update)
    const count = decoding.readVarUint(decoder)
    if (count > 1000) throw new Error('Too many awareness entries')
    const entries: { clock: number; state: string }[] = []
    for (let i = 0; i < count; i++) {
      const id = decoding.readVarUint(decoder)
      const clock = decoding.readVarUint(decoder)
      const state = JSON.parse(decoding.readVarString(decoder))
      // Clients may echo remote removals; discard those instead of trusting them.
      if (id !== this.access.clientId) continue
      if (state !== null && (typeof state !== 'object' || Array.isArray(state))) throw new Error('Invalid awareness')
      entries.push({ clock, state: JSON.stringify(state === null ? null : {
        ...state, user: { id: this.access.userId, name: this.access.name, color: color(this.access.userId) },
      }) })
    }
    const awareness = encoding.createEncoder()
    encoding.writeVarUint(awareness, entries.length)
    for (const entry of entries) {
      encoding.writeVarUint(awareness, this.access.clientId)
      encoding.writeVarUint(awareness, entry.clock)
      encoding.writeVarString(awareness, entry.state)
    }
    const message = encoding.createEncoder()
    encoding.writeVarUint(message, 1)
    encoding.writeVarUint8Array(message, encoding.toUint8Array(awareness))
    return encoding.toUint8Array(message)
  }
}

export function attachSecureWebSocket(server: Server, options: {
  authorize: (roomId: string, token: string) => Promise<Access | null>
  setup: (socket: AuthorizedSocket, req: IncomingMessage, roomId: string) => void
  allowedOrigin: string
  checkIntervalMs?: number
  leaseMs?: number
}) {
  const wss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 * 1024 })
  const connections = new Map<AuthorizedSocket, string>()
  const leaseMs = options.leaseMs ?? ACCESS_LEASE_MS
  const reject = (socket: Duplex, status: number) => {
    socket.end(`HTTP/1.1 ${status} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`)
  }
  const upgrade = async (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    // Retain an error handler while the async authorization request is pending.
    const onError = () => socket.destroy()
    socket.on('error', onError)
    try {
      const url = new URL(req.url ?? '/', 'http://localhost')
      const roomId = decodeURIComponent(url.pathname.slice(1))
      const token = url.searchParams.get('ticket')
      if (!/^[A-Za-z0-9_-]{1,128}$/.test(roomId) || !token || token.length > 2048 ||
        (req.headers.origin && req.headers.origin !== options.allowedOrigin)) return reject(socket, 401)
      const access = await options.authorize(roomId, token)
      if (socket.destroyed) return
      if (!access || access.expiresAt <= Date.now()) return reject(socket, 403)
      // Prevent a second socket from taking over a live client's awareness ID.
      if ([...connections].some(([conn, room]) => room === roomId && conn.access.clientId === access.clientId)) {
        return reject(socket, 409)
      }
      wss.handleUpgrade(req, socket, head, (ws) => {
        socket.off('error', onError)
        const conn = new AuthorizedSocket(ws, access, leaseMs)
        connections.set(conn, roomId)
        let checking = false
        const timer = setInterval(async () => {
          if (!conn.allowed() || checking) return
          checking = true
          try {
            const current = await options.authorize(roomId, token)
            if (!current || current.userId !== access.userId || current.clientId !== access.clientId ||
              current.role !== access.role) conn.close(4403, 'Room access changed')
            else if (conn.allowed()) conn.renew(leaseMs)
          } catch {
            conn.close(4403, 'Authorization unavailable')
          } finally { checking = false }
        }, options.checkIntervalMs ?? CHECK_INTERVAL_MS)
        const expiry = setTimeout(() => conn.close(4401, 'Ticket expired'), Math.max(1, access.expiresAt - Date.now()))
        conn.once('close', () => {
          connections.delete(conn)
          clearInterval(timer)
          clearTimeout(expiry)
        })
        try { options.setup(conn, req, roomId) }
        catch { conn.close(1011, 'Document initialization failed') }
      })
    } catch {
      if (!socket.destroyed) reject(socket, 503)
    }
  }
  server.on('upgrade', upgrade)
  return {
    wss,
    revoke(roomId: string, userId?: string) {
      for (const [conn, room] of connections) {
        if (room === roomId && (!userId || conn.access.userId === userId)) conn.close(4403, 'Room access changed')
      }
    },
    close() {
      server.off('upgrade', upgrade)
      for (const conn of connections.keys()) conn.close(1001, 'Server shutting down')
      for (const ws of wss.clients) ws.terminate()
      wss.close()
    },
  }
}
