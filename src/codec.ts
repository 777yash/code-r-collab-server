import * as Y from 'yjs'
import { gzipSync, gunzipSync } from 'zlib'

// Tagged snapshot container: [0x59, 0x5A, flags, ...payload].
// Untagged bytes are treated as legacy V1 updates (pre-compression snapshots).
const MAGIC_0 = 0x59 // 'Y'
const MAGIC_1 = 0x5a // 'Z'
const FLAG_V2 = 0x01
const FLAG_GZIP = 0x02

function isTagged(bytes: Uint8Array): boolean {
  return bytes.length >= 3 && bytes[0] === MAGIC_0 && bytes[1] === MAGIC_1
}

export function encodeSnapshot(doc: Y.Doc): Uint8Array {
  const update = Y.encodeStateAsUpdateV2(doc)
  const gz = gzipSync(Buffer.from(update), { level: 6 })
  const out = new Uint8Array(3 + gz.length)
  out[0] = MAGIC_0
  out[1] = MAGIC_1
  out[2] = FLAG_V2 | FLAG_GZIP
  out.set(gz, 3)
  return out
}

export function decodeInto(doc: Y.Doc, bytes: Uint8Array): void {
  if (!isTagged(bytes)) {
    Y.applyUpdate(doc, bytes)
    return
  }
  const flags = bytes[2]
  let payload = bytes.subarray(3)
  if (flags & FLAG_GZIP) {
    payload = new Uint8Array(gunzipSync(Buffer.from(payload)))
  }
  if (flags & FLAG_V2) Y.applyUpdateV2(doc, payload)
  else Y.applyUpdate(doc, payload)
}
