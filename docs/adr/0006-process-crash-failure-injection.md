# ADR 0006: Process-crash tests use inert injected hooks

Status: accepted

Milestone 3 must prove recovery across real process termination, not only
returned errors. Small hook functions are injected directly into capture,
acknowledgment, and delivery components by tests. Production composition passes
the zero value, so normal binaries contain no environment variable, signal,
HTTP endpoint, or configuration option that can activate a crash.

Tests run the ordinary persistence and delivery code in child test processes.
At a selected boundary, a hook calls `os.Exit` without running deferred cleanup.
The parent process then opens the same SQLite spool and verifies durable state
before replaying or retrying the operation.

The required boundaries are:

1. before the SQLite batch transaction;
2. after an event insert but before the SQLite batch commits;
3. after SQLite commits but before PostgreSQL acknowledgment;
4. immediately after the acknowledgment callback;
5. before a sink request;
6. while an HTTP request is in flight;
7. after a destination success but before SQLite records `delivered`.

An externally terminated in-flight request has an inherently ambiguous outcome:
the destination may have acted even when WriteRelay never received a response.
Recovery therefore retries the retained non-terminal delivery with the same
idempotency key. Tests must assert that duplicate requests are possible and must
not reinterpret this as exactly-once behavior.
