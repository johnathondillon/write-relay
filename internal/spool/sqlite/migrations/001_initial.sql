CREATE TABLE schema_migrations (
    version    INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE spool_metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE events (
    sequence          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_source      TEXT NOT NULL,
    event_id          TEXT NOT NULL,
    event_type        TEXT NOT NULL,
    subject           TEXT,
    payload           BLOB NOT NULL,
    payload_sha256    BLOB NOT NULL CHECK (length(payload_sha256) = 32),
    transaction_id    INTEGER NOT NULL,
    message_lsn       TEXT NOT NULL,
    commit_lsn        TEXT NOT NULL,
    commit_end_lsn    TEXT NOT NULL,
    commit_time       TEXT NOT NULL,
    message_index     INTEGER NOT NULL,
    captured_at       TEXT NOT NULL,
    UNIQUE (event_source, event_id)
);

CREATE INDEX events_transaction_order
    ON events (transaction_id, message_index);

INSERT INTO schema_migrations(version, applied_at)
VALUES (1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

