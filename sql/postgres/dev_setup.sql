-- Development-only objects and credentials. Never use these passwords outside
-- the disposable Docker Compose environment.
DO $block$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'writerelay_app') THEN
        CREATE ROLE writerelay_app LOGIN PASSWORD 'dev-app-password';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'writerelay_repl') THEN
        CREATE ROLE writerelay_repl LOGIN REPLICATION PASSWORD 'dev-repl-password';
    END IF;
END
$block$;

GRANT CONNECT ON DATABASE writerelay TO writerelay_app, writerelay_repl;
GRANT USAGE ON SCHEMA writerelay TO writerelay_app;
-- The daemon resolves the function during setup/doctor but does not execute it.
GRANT USAGE ON SCHEMA writerelay TO writerelay_repl;
GRANT EXECUTE ON FUNCTION writerelay.emit(jsonb) TO writerelay_app;

CREATE TABLE IF NOT EXISTS public.orders (
    id     text PRIMARY KEY,
    status text NOT NULL
);
GRANT SELECT, INSERT, UPDATE ON public.orders TO writerelay_app;

DO $block$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'writerelay_publication'
    ) THEN
        CREATE PUBLICATION writerelay_publication;
    END IF;
END
$block$;
