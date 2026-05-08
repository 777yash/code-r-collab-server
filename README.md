# collab-server

Real-time collaboration backend for **Code-R**.

- Maintains persistent WebSocket connections between users in the same room — something Vercel's serverless environment cannot do
- Syncs document edits instantly across all connected clients using Yjs CRDT, so simultaneous typing never causes conflicts
- Broadcasts cursor positions, handles reconnections, and snapshots document state to PostgreSQL so nothing is lost if the server restarts

**Live:** https://code-r-collab-server.onrender.com
