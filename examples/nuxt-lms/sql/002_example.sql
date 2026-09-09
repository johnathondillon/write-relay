-- Disposable local credentials. Two databases keep the service boundaries real.
CREATE ROLE lms_app LOGIN PASSWORD 'example-lms-password';
CREATE ROLE writerelay_repl LOGIN REPLICATION PASSWORD 'example-repl-password';
REVOKE CONNECT ON DATABASE writerelay FROM PUBLIC;
GRANT CONNECT ON DATABASE writerelay TO lms_app, writerelay_repl;
GRANT USAGE ON SCHEMA writerelay TO lms_app, writerelay_repl;
GRANT EXECUTE ON FUNCTION writerelay.emit(jsonb) TO lms_app;
CREATE PUBLICATION lms_example_publication;

CREATE TABLE completions (
    id uuid PRIMARY KEY,
    learner text NOT NULL,
    course_id text NOT NULL,
    course_title text NOT NULL,
    completed_at timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT, INSERT ON completions TO lms_app;

CREATE ROLE certificate_app LOGIN PASSWORD 'example-certificate-password';
CREATE DATABASE certificates OWNER certificate_app;
REVOKE CONNECT ON DATABASE certificates FROM PUBLIC;
GRANT CONNECT ON DATABASE certificates TO certificate_app;
\connect certificates
SET ROLE certificate_app;

CREATE TABLE inbox (
    idempotency_key text PRIMARY KEY,
    payload_sha256 text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE certificates (
    completion_id uuid PRIMARY KEY,
    certificate_id uuid NOT NULL UNIQUE,
    learner text NOT NULL,
    course_title text NOT NULL,
    issued_at timestamptz NOT NULL DEFAULT now()
);
-- This is receiver-side evidence, not WriteRelay's authoritative delivery state.
CREATE TABLE receipts (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    completion_id uuid NOT NULL,
    idempotency_key text NOT NULL,
    outcome text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
