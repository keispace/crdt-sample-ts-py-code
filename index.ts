import express from 'express';
import bodyParser from 'body-parser';
import { createClient } from '@libsql/client';
import * as Y from 'yjs';

const DOC_ID = 'demo-1';
const PORT = 3030;
const PEER_BASE = 'http://localhost:8000';

const db = createClient({
    url: 'file:node-js.db',
    authToken: undefined,
});

// ---------------- DDL (updates + snapshots) ----------------------
async function initDDL() {
    await db.execute(`
    CREATE TABLE IF NOT EXISTS updates (
      doc_id      TEXT    NOT NULL,
      seq         INTEGER PRIMARY KEY AUTOINCREMENT,
      update_data      BLOB    NOT NULL,
      origin      TEXT    DEFAULT '',
      received_at INTEGER NOT NULL
    );
  `);

    await db.execute(`
    CREATE TABLE IF NOT EXISTS snapshots (
      doc_id     TEXT PRIMARY KEY,
      snapshot_data   BLOB NOT NULL,   -- full-state encoded as a single Y update
      last_seq   INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);
}

// ---------------- DB helpers ------------------------------------
async function dbInsertUpdate(updateBytes: Uint8Array, origin = ''): Promise<number> {
    const updateBytesBuf = _u8Arr2Buf(updateBytes);
    const res = await db.execute({
        sql: `INSERT INTO updates (doc_id, update_data, origin, received_at)
          VALUES (?, ?, ?, ?)`,
        args: [DOC_ID, updateBytesBuf, origin, Date.now()],
    });
    return Number(res.lastInsertRowid || -1);
}

async function dbGetSnapshotRow(): Promise<{ snapBytes: Uint8Array | null; lastSeq: number }> {
    const q = await db.execute({
        sql: `SELECT snapshot_data, last_seq FROM snapshots WHERE doc_id=?`,
        args: [DOC_ID],
    });
    if (q.rows.length === 0) return { snapBytes: null, lastSeq: 0 };
    const row = q.rows[0];
    const snapshot_data = _buf2U8Arr(row.snapshot_data as unknown as Buffer);

    return await { snapBytes: snapshot_data, lastSeq: Number(row.last_seq || 0) };
}

async function dbUpsertSnapshot(snapshotBytes: Uint8Array, lastSeq: number): Promise<void> {
    const snapshotBytesBuf = _u8Arr2Buf(snapshotBytes);
    await db.execute({
        sql: `
      INSERT INTO snapshots (doc_id, snapshot_data, last_seq, updated_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(doc_id) DO UPDATE SET
        snapshot_data=excluded.snapshot_data,
        last_seq=excluded.last_seq,
        updated_at=excluded.updated_at
    `,
        args: [DOC_ID, snapshotBytesBuf, lastSeq, Date.now()],
    });
}

async function dbLoadUpdatesSince(seqExclusive: number): Promise<Array<{ seq: number; updateData: Uint8Array }>> {
    const q = await db.execute({
        sql: `SELECT seq, update_data FROM updates WHERE doc_id=? AND seq>? ORDER BY seq ASC`,
        args: [DOC_ID, seqExclusive],
    });
    const res = q.rows.map((r) => {
        const update_data = _buf2U8Arr(r.update_data as unknown as Buffer);
        return { seq: Number(r.seq), updateData: update_data };
    });
    return res;
}

async function dbDeleteUpdatesUpto(seqInclusive: number): Promise<number> {
    const del = await db.execute({
        sql: `DELETE FROM updates WHERE doc_id=? AND seq<=?`,
        args: [DOC_ID, seqInclusive],
    });
    return Number(del.rowsAffected || 0);
}

async function dbInit(): Promise<void> {
    await db.execute(`DELETE FROM updates WHERE doc_id=?`, [DOC_ID]);
    await db.execute(`DELETE FROM snapshots WHERE doc_id=?`, [DOC_ID]);
}

async function dbGetMaxSeq(): Promise<number> {
    const q = await db.execute({
        sql: `SELECT COALESCE(MAX(seq),0) FROM updates WHERE doc_id=?`,
        args: [DOC_ID],
    });
    if (q.rows.length === 0) return 0;
    const cur = q.rows[0];
    return Number(Object.values(cur)[0] || 0);
}

// ---------------- Core logic (endpoint-agnostic) -----------------
function _b642U8Arr(b64: string): Uint8Array {
    try {
        return _buf2U8Arr(Buffer.from(b64, 'base64'));
    } catch {
        throw new Error('invalid or base64 update');
    }
}

function _buf2U8Arr(buf: any): Uint8Array {
    if (buf instanceof Uint8Array) return buf;
    if (buf instanceof ArrayBuffer) return new Uint8Array(buf);
    if (Buffer.isBuffer(buf)) return new Uint8Array(buf);
    throw new Error(`not a buffer: ${typeof buf}`);
}

function _u8Arr2Buf(u8: Uint8Array): Buffer {
    if (Buffer.isBuffer(u8)) return u8;
    if (u8 instanceof ArrayBuffer) return Buffer.from(u8);
    if (u8 instanceof Uint8Array) return Buffer.from(u8);
    throw new Error(`not a Uint8Array: ${typeof u8}`);
}

async function applyIncomingUpdateB64(b64: string, origin = 'remote') {
    const update_bytes = _b642U8Arr(b64);
    if (!update_bytes || update_bytes.byteLength === 0) {
        throw new Error('empty base64 update');
    }
    const seq = await dbInsertUpdate(update_bytes, origin);
    return seq;
}

async function syncCompact(): Promise<{
    applied: number;
    deleted: number;
    before_last_seq: number;
    after_last_seq: number;
    snapshot_bytes: any;
}> {
    // 1) Load durable snapshot row
    const { snapBytes, lastSeq } = await dbGetSnapshotRow();

    // 2) Build a temp Y.Doc from durable snapshot
    const tmp = new Y.Doc();
    if (snapBytes && snapBytes.byteLength > 0) {
        Y.applyUpdate(tmp, snapBytes);
    }

    // 3) Apply all pending updates after lastSeq
    const pending = await dbLoadUpdatesSince(lastSeq);
    if (pending.length === 0) {
        // keep in-memory doc aligned to durable snapshot
        return {
            applied: 0,
            deleted: 0,
            before_last_seq: lastSeq,
            after_last_seq: lastSeq,
            snapshot_bytes: snapBytes ? snapBytes.byteLength : 0,
        };
    }

    let lastApplied = lastSeq;
    try {
        for (const { seq, updateData } of pending) {
            let updateBytes = updateData;
            lastApplied = seq;
            Y.applyUpdate(tmp, updateBytes);
        }
    } catch (e) {
        console.error('failed to apply pending updates:', e);
        throw new Error('failed to apply pending updates');
    }

    // 4) Produce a fresh full-state snapshot (diff from empty)
    const newSnapshot = Y.encodeStateAsUpdate(tmp);

    // 5) Upsert durable snapshot and delete applied updates
    await dbUpsertSnapshot(newSnapshot, lastApplied);
    const deleted = await dbDeleteUpdatesUpto(lastApplied);

    return {
        applied: pending.length,
        deleted,
        before_last_seq: lastSeq,
        after_last_seq: lastApplied,
        snapshot_bytes: newSnapshot.byteLength,
    };
}
function _crdtToJSON(val: Y.Map<any> | Y.Array<any>): any {
    if (!val) return null;
    if (val instanceof Y.Map) {
        const obj: Record<string, any> = {};
        for (const [k, v] of val.entries()) obj[k] = _crdtToJSON(v);
        return obj;
    }
    if (val instanceof Y.Array) {
        return val.toArray().map(_crdtToJSON);
    }
    return val;
}
// === (1) 유틸: durable 재구성 ===
// snapshot + updates를 BLOB로 읽어 임시 Doc을 재조립.
// (이미 사용 중인 dbGetSnapshotRow, dbLoadUpdatesSince 등을 그대로 활용)
async function rebuildDurableDoc(): Promise<Y.Doc> {
    const { snapBytes, lastSeq } = await dbGetSnapshotRow();
    const ydoc = new Y.Doc();
    if (snapBytes?.byteLength) Y.applyUpdate(ydoc, snapBytes);
    for (const { updateData } of await dbLoadUpdatesSince(lastSeq)) Y.applyUpdate(ydoc, updateData);
    return ydoc;
}
// ---------------- HTTP (thin endpoints) --------------------------
const app: any = express();
app.use(bodyParser.json());

app.get('/init', async (_req: any, res: any) => {
    console.log('GET /init');
    await dbInit();
    const ydoc = new Y.Doc();
    const root = ydoc.getMap('root');
    root.set('count', 1);
    root.set('message', 'Hello');
    ydoc.getArray('items');
    const initFullSnapshot = Y.encodeStateAsUpdate(ydoc);
    const maxSeq = await dbGetMaxSeq();
    await dbUpsertSnapshot(initFullSnapshot, maxSeq);
    res.json({ ok: true, seq: maxSeq, doc_id: DOC_ID });
});

app.get('/get-snapshot', async (_req: any, res: any) => {
    console.log('GET /get-snapshot');
    try {
        const { snapBytes } = await dbGetSnapshotRow();
        let dbJson = {};
        if (snapBytes && snapBytes.byteLength > 0) {
            const tmp = new Y.Doc();
            Y.applyUpdate(tmp, snapBytes);
            dbJson = {
                root: _crdtToJSON(tmp.getMap('root')),
                items: _crdtToJSON(tmp.getArray('items')),
            };
        }

        res.json({ ok: true, snapshot: dbJson });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'internal_error' });
    }
});

app.get('/add-count', async (_req: any, res: any) => {
    console.log('GET /add-count');
    const ydoc = new Y.Doc();
    const { snapBytes } = await dbGetSnapshotRow();
    if (snapBytes && snapBytes.byteLength > 0) {
        Y.applyUpdate(ydoc, snapBytes); // 임시 Doc 복원
    }
    const cur: number = (ydoc.getMap('root').get('count') as number) || 0;
    const svBefore: Uint8Array = Y.encodeStateVector(ydoc); // 이전 상태 추출
    ydoc.getMap('root').set('count', cur + 1); // 값 추가(상태 변경)
    const diff = Y.encodeStateAsUpdate(ydoc, svBefore); // 변경 증분만 추출
    await dbInsertUpdate(diff, 'local');

    res.json({ ok: true });
});

app.get('/sv', async (_req: any, res: any) => {
    // 내 state vector 반환
    console.log('GET /sv');
    const ydoc = await rebuildDurableDoc();
    const sv = Y.encodeStateVector(ydoc);
    res.json({ sv: Buffer.from(sv).toString('base64') });
});

app.post('/diff', async (req: any, res: any) => {
    // /diff: 입력 sv 기준 '상대가 모르는 diff' 반환 (없으면 null)
    console.log('POST /diff');
    const b64 = req.body?.sv;
    if (typeof b64 !== 'string') return res.json({ update: null });
    const sv = new Uint8Array(Buffer.from(b64, 'base64'));
    const ydoc = await rebuildDurableDoc();
    const diff = Y.encodeStateAsUpdate(ydoc, sv);
    res.json({ update: Buffer.from(diff).toString('base64') });
});

app.get('/do-sync', async (_req: any, res: any) => {
    console.log('GET /do-sync');
    // A) 로컬 컴팩션 (지금 하던 그대로)
    await syncCompact();

    // durable 문서 준비
    let ydoc = await rebuildDurableDoc();
    const svSelf = Y.encodeStateVector(ydoc);
    const svSelfB64 = Buffer.from(svSelf).toString('base64');

    // B) Pull: 내가 모르는 diff를 받기
    const r = await fetch(`${PEER_BASE}/diff`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ sv: svSelfB64 }),
    });
    const { update: diffB64 } = await r.json();
    if (diffB64) {
        const diffU8 = new Uint8Array(Buffer.from(diffB64, 'base64'));
        // DB append → 재컴팩션
        await dbInsertUpdate(Buffer.from(diffU8), 'pull');
        await syncCompact();
        // durable 재구성 (갱신된 스냅샷 기준으로 다시)
        ydoc = await rebuildDurableDoc();
    }

    // C) Push: 상대가 모르는 diff를 계산해 밀어넣기
    const r2 = await fetch(`${PEER_BASE}/sv`);
    const { sv: svPeerB64 } = await r2.json();
    if (svPeerB64) {
        const svPeer = new Uint8Array(Buffer.from(svPeerB64, 'base64'));
        const diffToPeer = Y.encodeStateAsUpdate(ydoc, svPeer);
        if (diffToPeer.byteLength > 0) {
            await fetch(`${PEER_BASE}/update`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ update: Buffer.from(diffToPeer).toString('base64') }),
            });
            // D) 원격 컴팩션
            await fetch(`${PEER_BASE}/compact`, { method: 'POST' });
        }
    }

    res.json({ ok: true });
});

app.post('/update', async (req: any, res: any) => {
    console.log('POST /update');
    try {
        const b64 = req.body?.update;
        if (typeof b64 !== 'string') {
            return res.status(400).json({ error: 'missing update (base64)' });
        }
        const seq = await applyIncomingUpdateB64(b64, 'remote');
        res.json({ ok: true, seq });
    } catch (e: any) {
        console.error('update error:', e);
        res.status(e.status || 500).json({ error: e.message || 'internal_error' });
    }
});

app.post('/compact', async (_req: any, res: any) => {
    console.log('POST /compact');
    try {
        const result = await syncCompact();
        res.json({ ok: true, result });
    } catch (e) {
        console.error('sync error:', e);
        res.status(500).json({ error: 'internal_error' });
    }
});

// ---------------- Boot -------------------------------------------
initDDL()
    .then(() => {
        app.listen(PORT, () => console.log(`Node listening on ${PORT} (doc_id=${DOC_ID})`));
    })
    .catch((e) => {
        console.error('DDL init failed:', e);
        process.exit(1);
    });
