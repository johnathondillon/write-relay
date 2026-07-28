# ADR 0003: SQLite is the durable acknowledgment boundary

Status: accepted

Every completed PostgreSQL transaction—including one with no matching
events—updates the SQLite checkpoint at its transaction-end LSN. Accepted events
and that checkpoint commit atomically under WAL journaling and
`synchronous=FULL`. Only then may a standby status update report the checkpoint.

The implementation uses `modernc.org/sqlite` v1.54, a CGO-free `database/sql`
driver, with one connection/writer, foreign keys, and a five-second busy timeout.
This favors portable static builds and a narrow persistence boundary. Driver
maintenance and performance should be reassessed before a stable release.

Spool storage loss, corruption beyond recovery, or dishonest `fsync` behavior is
outside the durability guarantee.

