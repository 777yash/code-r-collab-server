import * as Y from 'yjs'

const API_URL = process.env.NEXTJS_API_URL ?? 'http://localhost:3000'
const SECRET = process.env.NEXTJS_INTERNAL_SECRET ?? ''

const headers = () => ({ 'x-internal-secret': SECRET })

export async function loadSnapshot(roomId: string): Promise<Uint8Array | null> {
  try {
    const res = await fetch(`${API_URL}/api/rooms/${roomId}/snapshot`, {
      headers: headers(),
    })
    if (!res.ok) return null
    const buf = await res.arrayBuffer()
    if (buf.byteLength === 0) return null
    return new Uint8Array(buf)
  } catch {
    return null
  }
}

export async function saveSnapshot(roomId: string, doc: Y.Doc): Promise<void> {
  const update = Y.encodeStateAsUpdate(doc)
  try {
    await fetch(`${API_URL}/api/rooms/${roomId}/snapshot`, {
      method: 'PUT',
      headers: { ...headers(), 'content-type': 'application/octet-stream' },
      body: Buffer.from(update),
    })
  } catch (err) {
    console.error(`[snapshot] save failed for room ${roomId}:`, err)
  }
}

export async function saveAutoSnapshot(roomId: string, doc: Y.Doc): Promise<void> {
  const update = Y.encodeStateAsUpdate(doc)
  try {
    await fetch(`${API_URL}/api/rooms/${roomId}/snapshots/auto`, {
      method: 'POST',
      headers: { ...headers(), 'content-type': 'application/octet-stream' },
      body: Buffer.from(update),
    })
  } catch (err) {
    console.error(`[snapshot] auto-save failed for room ${roomId}:`, err)
  }
}
