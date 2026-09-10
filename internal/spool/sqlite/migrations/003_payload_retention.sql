ALTER TABLE events ADD COLUMN payload_pruned_at TEXT
    CHECK (payload_pruned_at IS NULL OR length(payload) = 0);

CREATE INDEX events_retained_payloads ON events(sequence)
    WHERE payload_pruned_at IS NULL;

CREATE TRIGGER events_prune_requires_delivered
BEFORE UPDATE OF payload_pruned_at ON events
WHEN OLD.payload_pruned_at IS NULL AND NEW.payload_pruned_at IS NOT NULL
 AND (NOT EXISTS (SELECT 1 FROM deliveries WHERE event_sequence = OLD.sequence)
      OR EXISTS (SELECT 1 FROM deliveries WHERE event_sequence = OLD.sequence
                 AND (state <> 'delivered' OR delivered_at IS NULL)))
BEGIN
    SELECT RAISE(ABORT, 'payload pruning requires all deliveries to have succeeded');
END;

CREATE TRIGGER events_pruned_payload_immutable
BEFORE UPDATE OF payload, payload_sha256, payload_pruned_at ON events
WHEN OLD.payload_pruned_at IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'pruned payload identity must remain intact');
END;

CREATE TRIGGER deliveries_require_payload
BEFORE INSERT ON deliveries
WHEN EXISTS (SELECT 1 FROM events WHERE sequence = NEW.event_sequence
             AND payload_pruned_at IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, 'cannot create a delivery for a pruned payload');
END;

CREATE TRIGGER deliveries_pruned_terminal
BEFORE UPDATE OF state ON deliveries
WHEN NEW.state <> 'delivered'
 AND EXISTS (SELECT 1 FROM events WHERE sequence = NEW.event_sequence
             AND payload_pruned_at IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, 'cannot redeliver a pruned payload');
END;

INSERT INTO schema_migrations(version, applied_at)
VALUES (3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
