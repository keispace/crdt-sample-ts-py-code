# install:  pip install fastapi uvicorn pycrdt requests
# run:      uvicorn app:app --reload --port 8000

import base64
import sqlite3
import threading
import time
from typing import List, Tuple, Optional

from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel

# --- CRDT runtime (pycrdt) ---
# Usage ref: https://y-crdt.github.io/pycrdt/usage/
from pycrdt import Doc as YDoc, Map as YMap, Array as YArr
import requests

DOC_ID = "demo-1"
DB_PATH = "file:node-py.db"
PEER_BASE = "http://localhost:3030"


# ---------------- SQLite schema (updates + snapshots) -------------
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute("""
CREATE TABLE IF NOT EXISTS updates (
doc_id          TEXT    NOT NULL,
seq             INTEGER PRIMARY KEY AUTOINCREMENT,
update_data     BLOB    NOT NULL,
origin          TEXT    DEFAULT '',
received_at     INTEGER NOT NULL
);
""")

db.execute("""
CREATE TABLE IF NOT EXISTS snapshots (
doc_id     TEXT PRIMARY KEY,
snapshot_data   BLOB NOT NULL,   -- full-state as a single update (Y-style)
last_seq   INTEGER NOT NULL,
updated_at INTEGER NOT NULL
);
""")
db.commit()
db_lock = threading.Lock()


# ---------------- DB helpers --------------------------------------
def db_insert_update(update_bytes: bytes, origin: str = "") -> int:
    global db, doc_lock
    with db_lock:
        cur = db.cursor()
        cur.execute(
            "INSERT INTO updates (doc_id, update_data, origin, received_at) VALUES (?, ?, ?, ?)",
            # (DOC_ID, sqlite3.Binary(update_bytes), origin, int(time.time() * 1000)),
            (DOC_ID, update_bytes, origin, int(time.time() * 1000)),
        )
        db.commit()
        return int(cur.lastrowid or -1)


def db_get_snapshot_row() -> Tuple[Optional[bytes], int]:
    global db, doc_lock
    with db_lock:
        cur = db.cursor()
        cur.execute(
            "SELECT snapshot_data, last_seq FROM snapshots WHERE doc_id=?", (DOC_ID,)
        )
        row = cur.fetchone()
        if not row:
            return None, 0
        return row[0], int(row[1])


def db_upsert_snapshot(snapshot_bytes: bytes, last_seq: int) -> None:
    global db, doc_lock
    with db_lock:
        db.execute(
            """
            INSERT INTO snapshots (doc_id, snapshot_data, last_seq, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(doc_id) DO UPDATE SET
              snapshot_data=excluded.snapshot_data,
              last_seq=excluded.last_seq,
              updated_at=excluded.updated_at
            """,
            # (DOC_ID, sqlite3.Binary(snapshot_bytes), last_seq, int(time.time() * 1000)),
            (DOC_ID, snapshot_bytes, last_seq, int(time.time() * 1000)),
        )
        db.commit()


def db_load_updates_since(seq_exclusive: int) -> List[Tuple[int, bytes]]:
    global db, doc_lock
    with db_lock:
        cur = db.cursor()
        cur.execute(
            "SELECT seq, update_data FROM updates WHERE doc_id=? AND seq>? ORDER BY seq ASC",
            (DOC_ID, seq_exclusive),
        )
        return [(int(s), u) for (s, u) in cur.fetchall()]


def db_delete_updates_upto(seq_inclusive: int) -> int:
    global db, doc_lock
    with db_lock:
        cur = db.cursor()
        cur.execute(
            "DELETE FROM updates WHERE doc_id=? AND seq<=?", (DOC_ID, seq_inclusive)
        )
        db.commit()
        return int(cur.rowcount or 0)


def db_init() -> None:
    global db, doc_lock
    with db_lock:
        cur = db.cursor()
        cur.execute("DELETE FROM updates WHERE doc_id=?", (DOC_ID,))
        cur.execute("DELETE FROM snapshots WHERE doc_id=?", (DOC_ID,))
    db.commit()


def db_get_max_seq() -> int:
    global db, doc_lock
    with db_lock:
        cur = db.cursor()
        cur.execute(
            "SELECT COALESCE(MAX(seq),0) FROM updates WHERE doc_id=?", (DOC_ID,)
        )
        return int(cur.fetchone()[0])


# ---------------- Core logic (endpoint-agnostic) -------------------
class UpdatePayload(BaseModel):
    update: str  # base64 (opaque Y/pycrdt update)


def apply_incoming_update_b64(b64: str, origin: str = "remote") -> int:
    """Decode base64 update and append to local updates table."""
    try:
        update_bytes = base64.b64decode(b64)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid base64 update")
    if not update_bytes:
        raise HTTPException(status_code=400, detail="empty update")
    seq = db_insert_update(update_bytes, origin=origin)
    return seq


def sync_compact() -> dict:
    """
    Read durable snapshot+updates from DB, replay to in-memory Doc,
    upsert new durable snapshot, and delete applied updates.
    """
    # 1) load durable snapshot row
    snap_bytes, last_seq = db_get_snapshot_row()

    # 2) build a temporary Doc from durable snapshot to compute new state
    tmp = YDoc(allow_multithreading=True)
    if snap_bytes and len(snap_bytes) > 0:
        tmp.apply_update(snap_bytes)

    # 3) apply all pending updates after last_seq
    pending = db_load_updates_since(last_seq)
    if not pending:
        return {
            "applied": 0,
            "deleted": 0,
            "before_last_seq": last_seq,
            "after_last_seq": last_seq,
            "snapshot_bytes": len(snap_bytes or b""),
        }

    last_applied = last_seq
    for seq, update_data in pending:
        tmp.apply_update(update_data)
        last_applied = seq

    # 4) produce a fresh full-state snapshot (diff from empty)
    new_snapshot = tmp.get_update()  # empty state -> full state

    # 5) upsert durable snapshot and delete applied updates
    db_upsert_snapshot(new_snapshot, last_seq=last_applied)
    deleted = db_delete_updates_upto(last_applied)

    return {
        "applied": len(pending),
        "deleted": deleted,
        "before_last_seq": last_seq,
        "after_last_seq": last_applied,
        "snapshot_bytes": len(new_snapshot),
    }


def _crdt_to_json(value):
    if isinstance(value, YMap):
        keys = list(value.keys())
        return {k: _crdt_to_json(value.get(k)) for k in keys}
    if isinstance(value, YArr):
        return [_crdt_to_json(v) for v in value]
    return value


def rebuild_durable_doc() -> "YDoc":
    """
    DB의 snapshot + updates를 순서대로 적용해 'durable' 문서를 임시로 재구성.
    (이미 sync_compact에서 하던 방식 재사용)
    """
    snapshot_bytes, last_seq = (
        db_get_snapshot_row()
    )  # (snapshot_bytes, last_seq) 튜플 가정
    updates = db_load_updates_since(last_seq)  # [(seq, bytes), ...] 가정
    doc = YDoc(allow_multithreading=True)
    if snapshot_bytes:
        doc.apply_update(snapshot_bytes)
    for _seq, u in updates:
        doc.apply_update(u)
    return doc


# ---------------- HTTP endpoints (thin) ----------------------------
app = FastAPI(title="CRDT sample (update + compact) with durable snapshots")


@app.get("/init")
def init_doc():
    db_init()
    ydoc = YDoc(allow_multithreading=True)
    ydoc["root"] = YMap({"count": 1, "message": "hello"})
    ydoc["items"] = YArr([])
    init_snapshot = ydoc.get_update()
    ydoc.apply_update(init_snapshot)
    max_seq = db_get_max_seq()
    db_upsert_snapshot(init_snapshot, last_seq=max_seq)
    return {"ok": True, "seq": max_seq, "doc_id": DOC_ID}


@app.get("/get-snapshot")
def get_snapshot():
    # durable snapshot (DB)
    snap_bytes, _last_seq = db_get_snapshot_row()
    if snap_bytes and len(snap_bytes) > 0:
        tmp = YDoc(allow_multithreading=True)
        tmp.apply_update(snap_bytes)
        db_json = {
            "root": _crdt_to_json(tmp.get("root", type=YMap)),
            "items": _crdt_to_json(tmp.get("items", type=YArr)),
        }
    else:
        db_json = None  # 아직 스냅샷 없음

    return {"ok": True, "snapshot": db_json}


@app.get("/add-count")
def add_count():
    tmp = YDoc(allow_multithreading=True)
    snap_bytes, _last_seq = db_get_snapshot_row()
    if snap_bytes and len(snap_bytes) > 0:
        tmp.apply_update(snap_bytes)  # 임시 Doc 복원
    root = tmp.get("root", type=YMap)

    cur = root.get("count", 0)
    sv_before = tmp.get_state()  # 이전 상태 추출
    root["count"] = int(cur) + 1  # 값 추가(상태 변경)
    diff = tmp.get_update(sv_before)  # 변경 증분만 추출
    db_insert_update(diff, origin="local")

    return {"ok": True}


@app.get("/sv")
def http_sv():
    # 현재 내 state vector 반환
    ydoc = rebuild_durable_doc()
    sv = ydoc.get_state()
    return {"sv": base64.b64encode(sv).decode("ascii")}


@app.post("/diff")
def http_diff(payload=Body(...)):
    #  입력 sv 기준 '상대가 모르는 diff' 반환 (없으면 None)
    b64 = payload.get("sv")
    if not isinstance(b64, str):
        return {"update": None}
    sv = base64.b64decode(b64)
    ydoc = rebuild_durable_doc()
    diff = ydoc.get_update(sv)
    if not diff:
        return {"update": None}
    return {"update": base64.b64encode(diff).decode("ascii")}


@app.get("/do-sync")
def do_sync():
    # A) 로컬 컴팩션 (지금 하던 그대로)
    sync_compact()

    # durable 문서 준비
    ydoc = rebuild_durable_doc()
    sv_self = ydoc.get_state()

    # B) Pull: 내가 모르는 diff를 받기
    r = requests.post(
        f"{PEER_BASE}/diff", json={"sv": base64.b64encode(sv_self).decode("ascii")}
    )
    r.raise_for_status()
    diff_b64 = (r.json() or {}).get("update")
    if diff_b64:
        diff = base64.b64decode(diff_b64)
        # DB에 append 후 재컴팩션
        db_insert_update(diff, origin="pull")
        sync_compact()

        # durable 재구성 (갱신된 스냅샷 기준으로 다시)
        ydoc = rebuild_durable_doc()

    # C) Push: 상대가 모르는 diff를 계산해 밀어넣기
    r2 = requests.get(f"{PEER_BASE}/sv")
    r2.raise_for_status()
    sv_peer = base64.b64decode((r2.json() or {}).get("sv", ""))
    diff_to_peer = ydoc.get_update(sv_peer)
    if diff_to_peer:
        requests.post(
            f"{PEER_BASE}/update",
            json={"update": base64.b64encode(diff_to_peer).decode("ascii")},
        ).raise_for_status()
        requests.post(f"{PEER_BASE}/compact").raise_for_status()  # D) 원격 컴팩션

    return {"ok": True}


@app.post("/update")
def http_update(payload: UpdatePayload):
    seq = apply_incoming_update_b64(payload.update, origin="remote")
    return {"ok": True, "seq": seq}


@app.post("/compact")
def http_compact():
    result = sync_compact()
    return {"ok": True, "result": result}
