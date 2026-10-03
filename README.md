# collab-server

Real-time collaboration backend for **Code-R** — the stateful half of a serverless code editor.

The [Next.js app](https://github.com/777yash/codeR) handles authentication, rooms, AI, and database access. This service holds the persistent WebSocket connections, keeps each room's Yjs document in sync, and saves its state through the app's internal snapshot API.

- **Conflict-free editing** — relays Yjs updates between collaborators working in the same multi-file workspace.
- **Authenticated WebSockets** — validates room tickets before opening a connection and enforces Owner / Editor / Viewer permissions on protocol messages.
- **Live awareness** — broadcasts cursors and selections using server-verified user identities, including across server instances.
- **Redis scaling** — relays edits and presence through Upstash pub/sub, synchronizes newly joined instances, and exchanges state again after reconnecting.
- **Snapshot persistence** — loads state on first join, saves on last leave, and autosaves changed documents every 30 seconds, including deletion-only updates.
- **Validated restores** — accepts supported V1/V2 snapshot formats, bounds compressed payloads, and preserves corrupt stored snapshots for recovery.

**Stack:** Node.js, TypeScript, ws, Yjs, y-websocket server utilities, y-protocols, lib0, ioredis, dotenv, and tsx. Snapshots are stored in Supabase Postgres through the Next.js app; this server does not connect directly to the database.

**Live:** [code-r-collab-server.onrender.com](https://code-r-collab-server.onrender.com) · **Web app:** [code-r-ruby.vercel.app](https://code-r-ruby.vercel.app)

## Setup

Use Node.js 20.19+ and npm, with the Code-R app configured and reachable.

```bash
git clone https://github.com/777yash/code-r-collab-server.git
cd code-r-collab-server
npm ci
cp .env.example .env
```

On PowerShell, use `Copy-Item .env.example .env` instead of `cp`. Configure:

```dotenv
PORT=1234
NEXTJS_API_URL=http://localhost:3000
COLLAB_ALLOWED_ORIGIN=http://localhost:3000
NEXTJS_INTERNAL_SECRET=""

# Optional for one server; required to synchronize multiple instances
UPSTASH_REDIS_URL=""
```

Set `NEXTJS_INTERNAL_SECRET` to the same randomly generated value on both services, at least 32 characters long. `NEXTJS_API_URL` must point to the reachable Next.js app; `COLLAB_ALLOWED_ORIGIN` must match the browser app's origin and defaults to the API URL's origin if omitted.

For Upstash, use the **native TCP/ioredis connection URL** (`rediss://:password@host:port`), not its HTTPS REST endpoint. `REDIS_URL` is also supported; `UPSTASH_REDIS_URL` takes precedence if both are set. Keep the URL in the ignored `.env` file or deployment environment. Without either value, the server runs in single-instance mode. Multiple replicas should use the same Redis database.

In the web app, set `NEXT_PUBLIC_COLLAB_WS_URL=ws://localhost:1234` and use the same app origin in `NEXT_PUBLIC_APP_URL`. Then start both services:

```bash
npm run dev        # TypeScript watch mode, http/ws://localhost:1234

# Production
npm run build
npm start
```

Deploy the web app and this server together. Use HTTPS/WSS for deployed URLs, and keep the app's internal authorization and snapshot endpoints reachable from every replica. The server's HTTP health response is `collab-server ok`; it confirms the HTTP process is running, not database or Redis health.

## How It Works

The signed-in browser requests a room-specific ticket valid for at most five minutes. Before upgrading a WebSocket, the server verifies the ticket through Next.js and checks current room access. Private rooms require membership; authenticated nonmembers of public rooms receive Viewer access. Owner/Editor connections can write shared state. Viewers can read and publish their own cursor, but cannot modify files, chat, or execution records in the shared document.

Access is rechecked every five seconds, with a ten-second authorization lease. Expired tickets, role changes, revoked access, and failed authorization checks close the connection; the browser obtains a fresh ticket on reconnect. Member and room changes also send a best-effort immediate revocation request. Removing a member from a public room still permits public Viewer access. Sign-out alone does not invalidate an already issued ticket before its expiry. Redact the `ticket` query parameter from proxy/access logs.

Each active room has an in-memory Yjs document. Redis relays updates and awareness to other instances, while state-vector exchanges bring joining or reconnecting peers up to date. Remote-origin markers prevent messages from echoing indefinitely. Redis synchronization complements persistence; it is not the durable snapshot store.

The server saves tagged **V2 + gzip** snapshots and can read legacy V1 and supported tagged V1/V2 formats. Snapshot bodies are capped at **5 MiB**, with gzip expansion capped at **32 MiB**. Full workspace state is validated before loading or restoring it. A corrupt stored snapshot closes affected connections and disables saves for that document so recovery can use a valid version or backup.

Autosave tracks document update revisions, so deletion-only edits are saved even when the Yjs state vector stays unchanged. Idle documents skip interval writes, failed saves stay pending for retry, and edits arriving during a save remain eligible for the next one. These are periodic snapshots; an abrupt process failure can still lose edits that have not been persisted.

## Verification

```bash
npm test
npm run build
```

Tests cover authentication, viewer write restrictions, awareness identity, snapshot validation and reset, deletion-only autosave, failed-save retries, and two-server document/presence synchronization. OAuth and Gist provider tests live in the web app repository.

The real Redis integration test is opt-in and reads the native connection URL from `.env`. It uses isolated, randomly named pub/sub channels and local WebSocket servers, without writing production room snapshots:

```bash
RUN_REDIS_INTEGRATION=1 npm test
```

On PowerShell, set `$env:RUN_REDIS_INTEGRATION='1'` before `npm test`, then use `Remove-Item Env:RUN_REDIS_INTEGRATION` afterward. Without the flag, the external Redis test is skipped and the remaining tests run locally.
