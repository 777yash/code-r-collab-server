import * as Y from 'yjs'
import { gunzipSync, gzipSync } from 'node:zlib'

// Keep this implementation identical in coder and collab-server.
// Legacy V1 or [0x59, 0x5a, flags, ...V1/V2 payload], optionally gzip-compressed.
export const MAX_SNAPSHOT_BYTES = 5 * 1024 * 1024
export const MAX_EXPANDED_SNAPSHOT_BYTES = 32 * 1024 * 1024
export const MAX_SNAPSHOT_JSON_BYTES =
  4 * Math.ceil(MAX_SNAPSHOT_BYTES / 3) + 4096
const MAGIC_0 = 0x59
const MAGIC_1 = 0x5a
const FLAG_V2 = 1
const FLAG_GZIP = 2

export class SnapshotValidationError extends Error {
  constructor(
    message = 'Invalid snapshot',
    public readonly status: 400 | 413 = 400
  ) {
    super(message)
    this.name = 'SnapshotValidationError'
  }
}
function invalid(): never {
  throw new SnapshotValidationError()
}

export function decodeSnapshotBase64(value: unknown): Buffer {
  if (typeof value !== 'string' || !value.length) invalid()
  if (value.length > 4 * Math.ceil(MAX_SNAPSHOT_BYTES / 3)) {
    throw new SnapshotValidationError('Snapshot too large', 413)
  }
  // Buffer.from is deliberately lenient; require canonical standard base64.
  if (value.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(value)) invalid()
  const bytes = Buffer.from(value, 'base64')
  if (bytes.toString('base64') !== value) invalid()
  if (bytes.length > MAX_SNAPSHOT_BYTES)
    throw new SnapshotValidationError('Snapshot too large', 413)
  return bytes
}
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
function isString(value: unknown): value is string {
  return typeof value === 'string'
}

/** Validate workspace types the editor consumes, including text-only file contents. */
export function validateSnapshotDocument(doc: Y.Doc): void {
  if (doc.store.pendingStructs || doc.store.pendingDs || doc.getSubdocs().size)
    invalid()
  for (const [name, root] of doc.share) {
    // Root types are untagged on the wire and initially become AbstractType.
    // Check map/sequence shape before materializing the expected type.
    if (
      name === 'file-list' ||
      name === 'execution-results' ||
      name === 'ai-control'
    ) {
      if (root._start !== null) invalid()
      const map = doc.getMap<unknown>(name)
      if (name === 'file-list') {
        if (map.size > 10_000) invalid()
        for (const [id, raw] of map) {
          if (!id || id.length > 256 || !isString(raw)) invalid()
          const meta: unknown = JSON.parse(raw)
          if (
            !isRecord(meta) ||
            meta.id !== id ||
            !isString(meta.name) ||
            !meta.name ||
            meta.name.length > 1024 ||
            /[\x00-\x1f]/.test(meta.name) ||
            /^[a-z]:/i.test(meta.name) ||
            meta.name
              .split(/[\\/]/)
              .some((part) => !part || part === '.' || part === '..') ||
            !isString(meta.language) ||
            !meta.language ||
            meta.language.length > 128 ||
            typeof meta.order !== 'number' ||
            !Number.isFinite(meta.order)
          )
            invalid()
        }
      } else {
        for (const value of map.values()) {
          if (name === 'ai-control') {
            if (typeof value !== 'boolean') invalid()
          } else {
            if (!isString(value)) invalid()
            const result: unknown = JSON.parse(value)
            if (
              !isRecord(result) ||
              !isString(result.stdout) ||
              !isString(result.stderr)
            )
              invalid()
            for (const key of ['exitCode', 'durationMs']) {
              if (
                result[key] !== undefined &&
                result[key] !== null &&
                (typeof result[key] !== 'number' ||
                  !Number.isFinite(result[key]))
              )
                invalid()
            }
            for (const key of ['execStatus', 'signal']) {
              if (
                result[key] !== undefined &&
                result[key] !== null &&
                !isString(result[key])
              )
                invalid()
            }
          }
        }
      }
    } else if (name === 'content' || name.startsWith('file:')) {
      if (
        root._map.size ||
        (name.startsWith('file:') && (name.length <= 5 || name.length > 261))
      )
        invalid()
      for (let item = root._start; item; item = item.right) {
        if (
          !item.deleted &&
          !(item.content instanceof Y.ContentString) &&
          !(item.content instanceof Y.ContentFormat)
        )
          invalid()
      }
      doc.getText(name)
    } else if (name === 'chat-messages') {
      if (root._map.size) invalid()
      for (const message of doc.getArray<unknown>(name)) {
        if (
          !isRecord(message) ||
          !['id', 'userId', 'userName', 'content'].every((key) =>
            isString(message[key])
          ) ||
          typeof message.timestamp !== 'number' ||
          !Number.isFinite(message.timestamp) ||
          (message.type !== undefined &&
            message.type !== 'text' &&
            message.type !== 'code') ||
          (message.language !== undefined && !isString(message.language))
        )
          invalid()
        if (message.ai !== undefined) {
          const ai = message.ai
          if (
            !isRecord(ai) ||
            !['generating', 'done', 'error'].includes(String(ai.status)) ||
            !isString(ai.triggeredBy) ||
            !isString(ai.reqId) ||
            (ai.files !== undefined &&
              (!Array.isArray(ai.files) || !ai.files.every(isString)))
          )
            invalid()
          if (
            ai.kind !== undefined &&
            ai.kind !== 'chat' &&
            ai.kind !== 'scaffold'
          )
            invalid()
          for (const key of ['buildCommand', 'startCommand']) {
            if (ai[key] !== undefined && !isString(ai[key])) invalid()
          }
        }
      }
    } else {
      invalid()
    }
  }
}

/** Caller must destroy the validated temporary document. No live state is mutated. */
export function readSnapshot(bytes: Uint8Array): Y.Doc {
  if (!bytes.length) invalid()
  if (bytes.length > MAX_SNAPSHOT_BYTES)
    throw new SnapshotValidationError('Snapshot too large', 413)
  const doc = new Y.Doc()
  try {
    const tagged =
      bytes.length >= 2 && bytes[0] === MAGIC_0 && bytes[1] === MAGIC_1
    const flags = tagged ? bytes[2] : 0
    if (tagged && (bytes.length < 4 || (flags & ~3) !== 0)) invalid()
    let payload = tagged ? bytes.subarray(3) : bytes
    if (flags & FLAG_GZIP) {
      try {
        payload = new Uint8Array(
          gunzipSync(payload, { maxOutputLength: MAX_EXPANDED_SNAPSHOT_BYTES })
        )
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ERR_BUFFER_TOO_LARGE') {
          throw new SnapshotValidationError('Expanded snapshot too large', 413)
        }
        throw error
      }
    }
    // Yjs otherwise accepts a valid update followed by junk.
    const Decoder = flags & FLAG_V2 ? Y.UpdateDecoderV2 : Y.UpdateDecoderV1
    let consumed: InstanceType<typeof Decoder>['restDecoder'] | undefined
    class CheckedDecoder extends Decoder {
      constructor(decoder: ConstructorParameters<typeof Decoder>[0]) {
        super(decoder)
        consumed = this.restDecoder
      }
    }
    Y.applyUpdateV2(doc, payload, undefined, CheckedDecoder)
    if (!consumed || consumed.pos !== payload.length) invalid()
    validateSnapshotDocument(doc)
    return doc
  } catch (error) {
    doc.destroy()
    if (error instanceof SnapshotValidationError) throw error
    throw new SnapshotValidationError()
  }
}
export function validateSnapshot(bytes: Uint8Array): void {
  const doc = readSnapshot(bytes)
  doc.destroy()
}
export function decodeInto(doc: Y.Doc, bytes: Uint8Array): void {
  const snapshot = readSnapshot(bytes)
  try {
    Y.applyUpdateV2(doc, Y.encodeStateAsUpdateV2(snapshot))
  } finally {
    snapshot.destroy()
  }
}
export function encodeSnapshot(doc: Y.Doc): Buffer {
  try {
    validateSnapshotDocument(doc)
    const update = Y.encodeStateAsUpdateV2(doc)
    if (update.length > MAX_EXPANDED_SNAPSHOT_BYTES) {
      throw new SnapshotValidationError('Expanded snapshot too large', 413)
    }
    const encoded = Buffer.concat([
      Buffer.from([MAGIC_0, MAGIC_1, FLAG_V2 | FLAG_GZIP]),
      gzipSync(update),
    ])
    if (encoded.length > MAX_SNAPSHOT_BYTES)
      throw new SnapshotValidationError('Snapshot too large', 413)
    return encoded
  } catch (error) {
    if (error instanceof SnapshotValidationError) throw error
    throw new SnapshotValidationError()
  }
}
