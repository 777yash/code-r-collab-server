import Redis from 'ioredis'
import * as Y from 'yjs'

// Random ID per process — prevents echoing own publishes back to self
const INSTANCE_ID = Math.random().toString(36).slice(2, 10)

let pub: Redis | null = null
let sub: Redis | null = null
let ready = false

// Channel → handler for incoming remote updates
const handlers = new Map<string, (update: Uint8Array) => void>()

/**
 * Call once at startup. Returns true if Redis is configured and connected.
 * When UPSTASH_REDIS_URL is absent, the server runs in single-instance mode
 * (all existing behaviour preserved — nothing breaks).
 */
export function initRedis(): boolean {
  const url = process.env.UPSTASH_REDIS_URL
  if (!url) {
    console.log('[redis] UPSTASH_REDIS_URL not set — running single-instance (pub/sub disabled)')
    return false
  }

  pub = new Redis(url, { lazyConnect: false, enableReadyCheck: true })
  sub = new Redis(url, { lazyConnect: false, enableReadyCheck: true })

  // Central dispatcher — one listener, routes by channel name
  sub.on('message', (channel: string, raw: string) => {
    const handler = handlers.get(channel)
    if (!handler) return
    try {
      const { id, update } = JSON.parse(raw) as { id: string; update: string }
      if (id === INSTANCE_ID) return // skip own echoes
      handler(Buffer.from(update, 'base64'))
    } catch (err) {
      console.error(`[redis] malformed message on ${channel}:`, err)
    }
  })

  sub.on('error', (err) => console.error('[redis] sub error:', err))
  pub.on('error', (err) => console.error('[redis] pub error:', err))

  ready = true
  console.log(`[redis] pub/sub ready — instance ${INSTANCE_ID}`)
  return true
}

/**
 * Wire a Yjs doc into the Redis pub/sub mesh for its room.
 * Must be called after initRedis(); no-ops gracefully when Redis is disabled.
 */
export function wireDocPubSub(docName: string, doc: Y.Doc): void {
  if (!ready || !pub || !sub) return

  const channel = `ydoc:${docName}`

  // applyingRemote prevents the update event triggered by Y.applyUpdate
  // from re-publishing back to Redis (infinite loop guard)
  let applyingRemote = false

  handlers.set(channel, (update: Uint8Array) => {
    applyingRemote = true
    try {
      Y.applyUpdate(doc, update)
    } finally {
      applyingRemote = false
    }
  })

  sub.subscribe(channel, (err) => {
    if (err) console.error(`[redis] subscribe error for ${channel}:`, err)
    else console.log(`[redis] subscribed to ${channel}`)
  })

  doc.on('update', (update: Uint8Array) => {
    if (applyingRemote) return
    pub!.publish(
      channel,
      JSON.stringify({ id: INSTANCE_ID, update: Buffer.from(update).toString('base64') }),
    ).catch((err) => console.error(`[redis] publish error for ${channel}:`, err))
  })

  doc.on('destroy', () => {
    handlers.delete(channel)
    sub!.unsubscribe(channel).catch(() => {})
    console.log(`[redis] unsubscribed from ${channel}`)
  })
}
