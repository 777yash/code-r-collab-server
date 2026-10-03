import { randomUUID } from 'node:crypto'
import * as Y from 'yjs'
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from 'y-protocols/awareness'

export interface MeshTransport {
  publish(channel: string, message: string): Promise<unknown>
  subscribe(channel: string): Promise<unknown>
  unsubscribe(channel: string): Promise<unknown>
  on(
    event: 'message',
    listener: (channel: string, message: string) => void
  ): unknown
  on(event: 'ready', listener: () => void): unknown
  off(
    event: 'message',
    listener: (channel: string, message: string) => void
  ): unknown
  off(event: 'ready', listener: () => void): unknown
}

const MAX_BYTES = 10 * 1024 * 1024
const REMOTE = Symbol('redis-peer')
type Room = { doc: Y.Doc; awareness?: Awareness; requestSync: () => void }

export function createRoomMesh(
  pub: Pick<MeshTransport, 'publish'>,
  sub: MeshTransport
) {
  const instance = randomUUID()
  const rooms = new Map<string, Room>()
  const cleanups = new Map<string, () => void>()
  const send = (channel: string, message: object) => {
    void pub
      .publish(channel, JSON.stringify({ ...message, id: instance }))
      .catch(() => console.error('[redis] room publish failed'))
  }
  const binary = (value: unknown) => {
    if (
      typeof value !== 'string' ||
      value.length > Math.ceil(MAX_BYTES / 3) * 4 ||
      value.length % 4 !== 0 ||
      !/^[A-Za-z0-9+/]*={0,2}$/.test(value)
    ) {
      throw new Error('Invalid mesh payload')
    }
    return Buffer.from(value, 'base64')
  }
  const encoded = (bytes: Uint8Array) => Buffer.from(bytes).toString('base64')
  const receive = (channel: string, raw: string) => {
    const room = rooms.get(channel)
    if (!room || raw.length > 2 * MAX_BYTES) return
    try {
      const message = JSON.parse(raw)
      if (typeof message?.id !== 'string' || message.id === instance) return
      if (message.to && message.to !== instance) return
      if (message.kind === 'sync') {
        send(channel, {
          kind: 'state',
          to: message.id,
          update: encoded(
            Y.encodeStateAsUpdate(room.doc, binary(message.vector))
          ),
          ...(room.awareness && {
            awareness: encoded(
              encodeAwarenessUpdate(
                room.awareness,
                Array.from(room.awareness.getStates().keys())
              )
            ),
          }),
        })
      } else if (
        message.kind === 'state' ||
        message.kind === 'update' ||
        (message.kind === undefined && typeof message.update === 'string')
      ) {
        Y.applyUpdate(room.doc, binary(message.update), REMOTE)
        if (message.awareness && room.awareness) {
          applyAwarenessUpdate(
            room.awareness,
            binary(message.awareness),
            REMOTE
          )
        }
      } else if (message.kind === 'awareness' && room.awareness) {
        applyAwarenessUpdate(room.awareness, binary(message.awareness), REMOTE)
      }
    } catch {
      console.error('[redis] invalid room message ignored')
    }
  }
  const reconnect = () => {
    for (const [channel, room] of rooms) {
      void sub
        .subscribe(channel)
        .then(() => {
          if (rooms.get(channel) === room) room.requestSync()
        })
        .catch(() => console.error('[redis] room resubscription failed'))
    }
  }
  sub.on('message', receive)
  sub.on('ready', reconnect)
  const wire = (name: string, doc: Y.Doc, awareness?: Awareness) => {
    const channel = `ydoc:${name}`
    if (rooms.has(channel)) throw new Error('Room already wired')
    const room: Room = {
      doc,
      awareness,
      requestSync: () => {
        send(channel, {
          kind: 'sync',
          vector: encoded(Y.encodeStateVector(doc)),
        })
        // Exchange both ways after reconnect to recover edits made during outages.
        send(channel, {
          kind: 'update',
          update: encoded(Y.encodeStateAsUpdate(doc)),
        })
        if (awareness)
          send(channel, {
            kind: 'awareness',
            awareness: encoded(
              encodeAwarenessUpdate(
                awareness,
                Array.from(awareness.getStates().keys())
              )
            ),
          })
      },
    }
    rooms.set(channel, room)
    const update = (bytes: Uint8Array, origin: unknown) => {
      if (origin !== REMOTE)
        send(channel, { kind: 'update', update: encoded(bytes) })
    }
    const awarenessUpdate = (
      {
        added,
        updated,
        removed,
      }: {
        added: number[]
        updated: number[]
        removed: number[]
      },
      origin: unknown
    ) => {
      if (origin !== REMOTE && awareness)
        send(channel, {
          kind: 'awareness',
          awareness: encoded(
            encodeAwarenessUpdate(awareness, [...added, ...updated, ...removed])
          ),
        })
    }
    doc.on('update', update)
    awareness?.on('update', awarenessUpdate)
    void sub
      .subscribe(channel)
      .then(() => {
        if (rooms.get(channel) === room) room.requestSync()
      })
      .catch(() => console.error('[redis] room subscription failed'))
    const cleanup = () => {
      doc.off('update', update)
      doc.off('destroy', cleanup)
      awareness?.off('update', awarenessUpdate)
      rooms.delete(channel)
      cleanups.delete(channel)
      void sub.unsubscribe(channel).catch(() => {})
    }
    cleanups.set(channel, cleanup)
    doc.on('destroy', cleanup)
  }
  return {
    wire,
    close: () => {
      sub.off('message', receive)
      sub.off('ready', reconnect)
      for (const cleanup of cleanups.values()) cleanup()
    },
  }
}
