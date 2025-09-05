# CRDT Durable Sample (Yjs/pycrdt, Node.js + FastAPI)

## Overview

- Real-time synchronization server sample based on **CRDT (Conflict-free Replicated Data Type)**
- Implemented with Node.js (Yjs) and Python (pycrdt), durability guaranteed with SQLite
- Provides incremental updates, snapshots, synchronization, bidirectional interworking, and API test examples

## Main Features

- `/init`: Document initialization and snapshot creation
- `/add-count`: Increase count value (state change)
- `/do-sync`: Bidirectional synchronization (exchange diffs with the other server)
- `/get-snapshot`: View current snapshot (JSON)
- `/update`, `/compact`, `/diff`, `/sv` and other CRDT sync-related endpoints provided

## Folder/File Structure

- `index.ts`: Node.js (Yjs) server implementation
- `app.py`: Python (pycrdt) server implementation
- `test.ts`: Test script for both servers' operation and synchronization
- `http.http`: HTTP request samples for REST API testing
- `test.ddl.sql`: DB table schema (SQL)
- `node-js.db`, `node-py.db`: SQLite DB files for each server

## Installation & Run

### 1. Node.js Server (Yjs)

```bash
npm install
npm run start-ts
# or
npx ts-node index.ts
```

### 2. Python Server (pycrdt)

```bash
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

### 3. Test

- Run `test.ts`: Both Node.js and Python servers must be running

```bash
ts-node test.ts
```

- Or run the `http.http` file with a REST Client (e.g., VSCode REST Client extension)

## Dependencies

- Node.js: express, body-parser, @libsql/client, yjs, typescript, etc.
- Python: fastapi, uvicorn, pycrdt, requests, etc.

## References

- Yjs: https://github.com/yjs/yjs
- pycrdt: https://y-crdt.github.io/pycrdt/
- CRDT concept: https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type
