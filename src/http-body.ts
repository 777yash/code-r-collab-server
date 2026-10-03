import type { IncomingMessage } from 'node:http'
import { SnapshotValidationError } from './codec.js'

export function readBody(req: IncomingMessage, limit: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    let size = 0
    let exceeded = false
    req.on('data', (chunk: Buffer) => {
      if (exceeded) return
      size += chunk.length
      if (size > limit) {
        exceeded = true
        chunks.length = 0
        reject(new SnapshotValidationError('Snapshot too large', 413))
        return
      }
      chunks.push(Buffer.from(chunk))
    })
    req.on('end', () => {
      if (!exceeded) resolve(Buffer.concat(chunks, size))
    })
    req.on('error', () =>
      reject(new SnapshotValidationError('Invalid request body'))
    )
    req.on('aborted', () =>
      reject(new SnapshotValidationError('Invalid request body'))
    )
  })
}
