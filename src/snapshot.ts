import * as Y from 'yjs'
import {
  encodeSnapshot,
  MAX_SNAPSHOT_BYTES,
  SnapshotValidationError,
} from './codec.js'
import { readLimitedBody } from './snapshot-body.js'

const API_URL = process.env.NEXTJS_API_URL ?? 'http://localhost:3000'
const SECRET = process.env.NEXTJS_INTERNAL_SECRET ?? ''

const headers = () => ({ 'x-internal-secret': SECRET })

export async function loadSnapshot(roomId: string): Promise<Uint8Array | null> {
  try {
    const res = await fetch(`${API_URL}/api/rooms/${roomId}/snapshot`, {
      headers: headers(),
    })
    if (!res.ok) return null
    if (res.status === 204) return null
    const buf = await readLimitedBody(res, MAX_SNAPSHOT_BYTES)
    return new Uint8Array(buf)
  } catch (error) {
    if (error instanceof SnapshotValidationError) throw error
    return null
  }
}

export async function saveSnapshot(
  roomId: string,
  doc: Y.Doc
): Promise<boolean> {
  try {
    const update = encodeSnapshot(doc)
    const response = await fetch(`${API_URL}/api/rooms/${roomId}/snapshot`, {
      method: 'PUT',
      headers: { ...headers(), 'content-type': 'application/octet-stream' },
      body: Buffer.from(update),
      signal: AbortSignal.timeout(10_000),
    })
    return response.ok
  } catch (err) {
    console.error(`[snapshot] save failed for room ${roomId}:`, err)
    return false
  }
}

export async function saveAutoSnapshot(
  roomId: string,
  doc: Y.Doc
): Promise<boolean> {
  try {
    const update = encodeSnapshot(doc)
    const response = await fetch(
      `${API_URL}/api/rooms/${roomId}/snapshots/auto`,
      {
        method: 'POST',
        headers: { ...headers(), 'content-type': 'application/octet-stream' },
        body: Buffer.from(update),
        signal: AbortSignal.timeout(10_000),
      }
    )
    return response.ok
  } catch (err) {
    console.error(`[snapshot] auto-save failed for room ${roomId}:`, err)
    return false
  }
}
