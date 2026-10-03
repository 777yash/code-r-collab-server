import { SnapshotValidationError } from './codec.js'

/** Bound the stream itself, including chunked requests without Content-Length. */
export async function readLimitedBody(
  source: Request | Response,
  limit: number
): Promise<Buffer> {
  const declared = Number(source.headers.get('content-length'))
  if (Number.isFinite(declared) && declared > limit) {
    throw new SnapshotValidationError('Snapshot too large', 413)
  }
  const reader = source.body?.getReader()
  if (!reader) return Buffer.alloc(0)
  const chunks: Buffer[] = []
  let size = 0
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) return Buffer.concat(chunks, size)
      size += value.byteLength
      if (size > limit) {
        await reader.cancel().catch(() => undefined)
        throw new SnapshotValidationError('Snapshot too large', 413)
      }
      chunks.push(Buffer.from(value))
    }
  } catch (error) {
    if (error instanceof SnapshotValidationError) throw error
    throw new SnapshotValidationError('Invalid snapshot body')
  } finally {
    reader.releaseLock()
  }
}
