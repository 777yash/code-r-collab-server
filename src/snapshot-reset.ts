import type { IncomingMessage, ServerResponse } from 'node:http'
import type * as Y from 'yjs'
import {
  decodeSnapshotBase64,
  MAX_SNAPSHOT_JSON_BYTES,
  SnapshotValidationError,
  validateSnapshot,
} from './codec.js'
import { readBody } from './http-body.js'
import { restoreLiveSnapshot } from './snapshot-state.js'

export function createResetHandler(
  isInternal: (req: IncomingMessage) => boolean,
  docs: Map<string, Y.Doc>
) {
  return async (
    req: IncomingMessage,
    res: ServerResponse,
    roomId: string
  ): Promise<void> => {
    if (!isInternal(req)) {
      res.writeHead(401)
      res.end()
      return
    }
    try {
      const raw = await readBody(req, MAX_SNAPSHOT_JSON_BYTES)
      let body: unknown
      try {
        body = JSON.parse(raw.toString('utf8'))
      } catch {
        throw new SnapshotValidationError('Invalid JSON')
      }
      const bytes = decodeSnapshotBase64(
        typeof body === 'object' && body !== null && 'data' in body
          ? body.data
          : undefined
      )
      const doc = docs.get(roomId)
      // Invalid snapshots are rejected even if there is no live document.
      if (doc) restoreLiveSnapshot(doc, bytes)
      else validateSnapshot(bytes)
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(
        JSON.stringify(
          doc ? { applied: true } : { applied: false, reason: 'no-live-doc' }
        )
      )
    } catch (error) {
      const invalid = error instanceof SnapshotValidationError
      if (!invalid) console.error('[restore] reset failed:', error)
      res.writeHead(invalid ? error.status : 500, {
        'content-type': 'application/json',
      })
      res.end(
        JSON.stringify({
          error: invalid ? error.message : 'Snapshot reset failed',
        })
      )
    }
  }
}
