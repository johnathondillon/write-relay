# ADR 0002: Built-in pgoutput with an empty publication

Status: accepted

The capture connection uses PostgreSQL's built-in `pgoutput` plugin, protocol
version 1, `messages=true`, and an empty publication. No external output plugin,
custom extension, transaction streaming, or two-phase decoding is required.

Slot and publication identifiers pass a conservative lowercase identifier rule
before use in protocol/plugin arguments. Unexpected relation or DML messages
produce a drift warning and are ignored.

Protocol version 1 keeps the state machine to Begin, Message, and Commit for the
small bounded events in Milestone 1.

