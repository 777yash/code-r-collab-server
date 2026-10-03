import type * as Y from 'yjs'

export function scheduleSnapshots(
  doc: Y.Doc,
  canSave: () => boolean,
  save: () => Promise<boolean>,
  intervalMs: number
) {
  let revision = 0
  let savedRevision = -1
  let saving = false
  let destroyed = false
  const changed = () => {
    revision++
  }
  doc.on('update', changed)
  const tick = async () => {
    if (destroyed || saving || !canSave() || savedRevision === revision) return
    saving = true
    const attemptedRevision = revision
    try {
      if (await save()) savedRevision = attemptedRevision
    } catch {
      console.error('[persistence] scheduled save failed')
    } finally {
      saving = false
    }
  }
  const timer = setInterval(() => {
    void tick()
  }, intervalMs)
  doc.on('destroy', () => {
    destroyed = true
    clearInterval(timer)
    doc.off('update', changed)
  })
  return tick
}
