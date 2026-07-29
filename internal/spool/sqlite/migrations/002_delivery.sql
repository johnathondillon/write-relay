CREATE TABLE delivery_sinks (
    sink_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    sink_name      TEXT NOT NULL UNIQUE,
    sink_type      TEXT NOT NULL,
    config_sha256  BLOB NOT NULL CHECK (length(config_sha256) = 32),
    active         INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at     TEXT NOT NULL
);

CREATE TABLE deliveries (
    event_sequence    INTEGER NOT NULL,
    sink_id           INTEGER NOT NULL,
    state             TEXT NOT NULL DEFAULT 'pending'
                      CHECK (state IN ('pending', 'retry_wait', 'delivered', 'dead_letter')),
    attempts          INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at   TEXT NOT NULL,
    last_attempt_at   TEXT,
    delivered_at      TEXT,
    dead_lettered_at  TEXT,
    last_error        TEXT,
    response_status   INTEGER,
    PRIMARY KEY (event_sequence, sink_id),
    FOREIGN KEY (event_sequence) REFERENCES events(sequence) ON DELETE RESTRICT,
    FOREIGN KEY (sink_id) REFERENCES delivery_sinks(sink_id) ON DELETE RESTRICT
);

CREATE INDEX deliveries_due
    ON deliveries (state, next_attempt_at, event_sequence, sink_id);

CREATE INDEX deliveries_sink_order
    ON deliveries (sink_id, event_sequence, state);

INSERT INTO schema_migrations(version, applied_at)
VALUES (2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
