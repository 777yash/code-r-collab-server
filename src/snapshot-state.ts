import * as Y from 'yjs'
import { decodeInto, readSnapshot, validateSnapshotDocument } from './codec.js'

export function createSnapshotLoader(
  load: (roomId: string) => Promise<Uint8Array | null>,
  onFailure: (roomId: string, doc: Y.Doc, error: unknown) => void
) {
  const failed = new WeakSet<Y.Doc>()
  return {
    canSave: (doc: Y.Doc) => !failed.has(doc) && !doc.isDestroyed,
    async bindState(roomId: string, doc: Y.Doc): Promise<void> {
      try {
        const bytes = await load(roomId)
        if (doc.isDestroyed) return
        if (bytes) decodeInto(doc, bytes)
      } catch (error) {
        // Mark before closing sockets: last-leave persistence must not overwrite
        // a corrupt stored snapshot with the uninitialized live document.
        failed.add(doc)
        try {
          onFailure(roomId, doc, error)
        } catch (reportError) {
          console.error('[persistence] failure reporting failed:', reportError)
        }
      }
    },
  }
}

/** Preflight every read/type before the transaction, which cannot roll back. */
export function restoreLiveSnapshot(doc: Y.Doc, bytes: Uint8Array): void {
  const snapshot = readSnapshot(bytes)
  try {
    validateSnapshotDocument(doc)
    const files = Array.from(
      snapshot.getMap<string>('file-list'),
      ([id, meta]) => ({
        id,
        meta,
        target: snapshot.getText(`file:${id}`).toString(),
        live: doc.getText(`file:${id}`),
      })
    )
    const hasLegacy = snapshot.share.has('content')
    const legacyTarget = hasLegacy
      ? snapshot.getText('content').toString()
      : null
    const legacyLive = hasLegacy ? doc.getText('content') : null
    const liveFiles = doc.getMap<string>('file-list')
    const restoredIds = new Set(files.map((file) => file.id))
    const removedIds = Array.from(liveFiles.keys()).filter(
      (id) => !restoredIds.has(id)
    )
    doc.transact(() => {
      for (const id of removedIds) liveFiles.delete(id)
      for (const { id, meta, target, live } of files) {
        liveFiles.set(id, meta)
        if (live.toString() !== target) {
          live.delete(0, live.length)
          live.insert(0, target)
        }
      }
      if (
        legacyLive &&
        legacyTarget !== null &&
        legacyLive.toString() !== legacyTarget
      ) {
        legacyLive.delete(0, legacyLive.length)
        legacyLive.insert(0, legacyTarget)
      }
    })
  } finally {
    snapshot.destroy()
  }
}
