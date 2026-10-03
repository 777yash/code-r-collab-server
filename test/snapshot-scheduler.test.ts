import assert from 'node:assert/strict'
import test from 'node:test'
import * as Y from 'yjs'
import { scheduleSnapshots } from '../src/snapshot-scheduler'

test('idle ticks skip uploads but deletion-only edits still save', async () => {
  const doc = new Y.Doc()
  let saves = 0
  const tick = scheduleSnapshots(
    doc,
    () => true,
    async () => {
      saves++
      return true
    },
    60_000
  )
  try {
    doc.getText('content').insert(0, 'text')
    await tick()
    await tick()
    assert.equal(saves, 1)
    const vector = Y.encodeStateVector(doc)
    doc.getText('content').delete(0, 4)
    assert.deepEqual(Y.encodeStateVector(doc), vector)
    await tick()
    assert.equal(saves, 2)
  } finally {
    doc.destroy()
  }
})
test('failed saves retry and updates during an upload remain dirty', async () => {
  const doc = new Y.Doc()
  let saves = 0
  let release: (ok: boolean) => void = () => {}
  const tick = scheduleSnapshots(
    doc,
    () => true,
    async () => {
      saves++
      return new Promise<boolean>((resolve) => {
        release = resolve
      })
    },
    60_000
  )
  try {
    const failed = tick()
    release(false)
    await failed
    const inFlight = tick()
    doc.getText('content').insert(0, 'new edit')
    await tick()
    assert.equal(saves, 2)
    release(true)
    await inFlight
    const followup = tick()
    release(true)
    await followup
    assert.equal(saves, 3)
    await tick()
    assert.equal(saves, 3)
  } finally {
    doc.destroy()
  }
})
test('invalid startup snapshots and destroyed docs never upload', async () => {
  const doc = new Y.Doc()
  let saves = 0
  const tick = scheduleSnapshots(
    doc,
    () => false,
    async () => {
      saves++
      return true
    },
    60_000
  )
  await tick()
  doc.destroy()
  await tick()
  assert.equal(saves, 0)
})
