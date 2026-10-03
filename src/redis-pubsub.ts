import Redis from 'ioredis'
import * as Y from 'yjs'
import type { Awareness } from 'y-protocols/awareness'
import { createRoomMesh, type MeshTransport } from './room-mesh'

let mesh: ReturnType<typeof createRoomMesh> | null = null
let pub: Redis | null = null
let sub: Redis | null = null

export function initRedis(): boolean {
  const url = process.env.UPSTASH_REDIS_URL || process.env.REDIS_URL
  if (!url) {
    console.log('[redis] single-instance mode (pub/sub disabled)')
    return false
  }
  pub = new Redis(url, { enableReadyCheck: true })
  sub = new Redis(url, { enableReadyCheck: true })
  pub.on('error', () => console.error('[redis] publisher connection failed'))
  sub.on('error', () => console.error('[redis] subscriber connection failed'))
  mesh = createRoomMesh(pub, sub as unknown as MeshTransport)
  sub.once('ready', () => console.log('[redis] pub/sub connected'))
  return true
}

export function wireDocPubSub(name: string, doc: Y.Doc): void {
  mesh?.wire(name, doc, (doc as Y.Doc & { awareness?: Awareness }).awareness)
}

export function closeRedis(): void {
  mesh?.close()
  pub?.disconnect()
  sub?.disconnect()
}
