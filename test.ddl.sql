CREATE TABLE IF NOT EXISTS updates (
    doc_id      TEXT    NOT NULL,
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    update_data      BLOB    NOT NULL,
    origin      TEXT    DEFAULT '',
    received_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    doc_id     TEXT PRIMARY KEY,
    snapshot_data   BLOB NOT NULL,   -- full-state encoded as a single Y update
    last_seq   INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);