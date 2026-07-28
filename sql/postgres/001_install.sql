-- WriteRelay's generic PostgreSQL installation. This script intentionally
-- creates no users, passwords, publication, slot, or business tables.
CREATE SCHEMA IF NOT EXISTS writerelay;

CREATE OR REPLACE FUNCTION writerelay.emit(event jsonb)
RETURNS pg_lsn
LANGUAGE plpgsql
VOLATILE
STRICT
SECURITY INVOKER
PARALLEL UNSAFE
SET search_path = pg_catalog
AS $function$
DECLARE
    payload_text  text;
    payload_bytes integer;
BEGIN
    IF jsonb_typeof(event) <> 'object' THEN
        RAISE EXCEPTION 'WriteRelay event must be a JSON object'
            USING ERRCODE = '22023';
    END IF;

    IF jsonb_typeof(event->'specversion') IS DISTINCT FROM 'string'
       OR event->>'specversion' IS DISTINCT FROM '1.0' THEN
        RAISE EXCEPTION 'WriteRelay specversion must be 1.0'
            USING ERRCODE = '22023';
    END IF;

    IF jsonb_typeof(event->'id') IS DISTINCT FROM 'string'
       OR COALESCE(event->>'id', '') = '' THEN
        RAISE EXCEPTION 'WriteRelay event id is required'
            USING ERRCODE = '22023';
    END IF;

    IF jsonb_typeof(event->'source') IS DISTINCT FROM 'string'
       OR COALESCE(event->>'source', '') = '' THEN
        RAISE EXCEPTION 'WriteRelay event source is required'
            USING ERRCODE = '22023';
    END IF;

    IF jsonb_typeof(event->'type') IS DISTINCT FROM 'string'
       OR COALESCE(event->>'type', '') = '' THEN
        RAISE EXCEPTION 'WriteRelay event type is required'
            USING ERRCODE = '22023';
    END IF;

    payload_text := event::text;
    payload_bytes := octet_length(convert_to(payload_text, 'UTF8'));

    IF payload_bytes > 262144 THEN
        RAISE EXCEPTION 'WriteRelay event exceeds 262144-byte limit'
            USING ERRCODE = '54000';
    END IF;

    RETURN pg_logical_emit_message(
        true,
        'writerelay.v1',
        payload_text
    );
END;
$function$;

REVOKE ALL ON FUNCTION writerelay.emit(jsonb) FROM PUBLIC;

COMMENT ON SCHEMA writerelay IS
    'Transactional domain-event emission API for WriteRelay';
COMMENT ON FUNCTION writerelay.emit(jsonb) IS
    'Emits one validated event as a transactional writerelay.v1 logical WAL message. Grant EXECUTE explicitly to application roles.';

-- Example production grant, executed by an administrator:
-- GRANT USAGE ON SCHEMA writerelay TO your_application_role;
-- GRANT EXECUTE ON FUNCTION writerelay.emit(jsonb) TO your_application_role;
