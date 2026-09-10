# Frequently asked questions

## What problem does WriteRelay solve?

Suppose an LMS saves a course completion and then calls a certificate service.
The LMS could crash after saving the completion but before making the call,
leaving no durable record that a notification still needs to be sent.

With WriteRelay, the LMS saves the completion and calls `writerelay.emit(...)`
inside the same PostgreSQL transaction. This saves the business change and the
notification intent together. The daemon captures that event into its local
SQLite spool and handles delivery, bounded retries, and retained dead letters.

## What commits together, and what happens later?

The application controls a PostgreSQL transaction containing its business SQL
and the event emission. Either both commit or both roll back.

The daemon captures committed events afterward. SQLite persistence and external
delivery are separate steps. A destination failure does not roll back the
application's PostgreSQL transaction, and WriteRelay does not make two services
succeed or fail together.

## Why does SQLite commit before PostgreSQL is acknowledged?

The application's PostgreSQL transaction has already committed. The daemon's
acknowledgment tells PostgreSQL how far the relay has durably captured the log;
it does not commit the application's transaction.

SQLite must save the events and their log-position checkpoint before that
acknowledgment allows PostgreSQL to release log data needed for recovery.
Otherwise, a crash could lose an event that PostgreSQL believes is safely
captured. If SQLite commits and the acknowledgment is lost, the event is safe
locally and identical replay is harmless.

## How would this work with a payment and course access?

For an application integrating with a payment provider such as Stripe:

1. The payment provider processes the payment.
2. Your application's payment-notification handler records the payment and
   emits `payment.recorded` in one PostgreSQL transaction.
3. WriteRelay captures the committed event and sends it to your course-access
   endpoint.
4. The endpoint grants access, or durably queues that work, before returning
   success.

The original payment is outside WriteRelay's transaction guarantee. If step 2
fails, your application must recover the payment notification or reconcile the
payment. WriteRelay does not receive payment-provider webhooks itself or undo
external payments. Your notification handler also needs to handle duplicates.

## Are retries automatic, manual, or application code?

The running daemon automatically retries network failures and HTTP `408`,
`425`, `429`, and `5xx` responses with bounded backoff. The default retry policy
allows 10 attempts, with an initial delay of one second and a maximum delay of
five minutes. These settings are configurable.

Other HTTP failures and exhausted retries move the delivery to retained
`dead_letter` state. After addressing the problem, an operator can use
`spool redrive` to make it eligible for delivery again. See the
[README's delivery configuration and commands](../README.md#configure-delivery).

Your application does not need to implement WriteRelay's delivery retry loop.
It does need to put the information the receiver needs, such as user and course
IDs, into the event. WriteRelay preserves the original event bytes for later
attempts; it does not reconstruct missing business data. Permanent failures
can still require debugging or operator action.

## What does a successful webhook response promise?

Any `2xx` response, including `202 Accepted`, tells WriteRelay that delivery
succeeded. The receiver should return it only after completing the work or
durably saving the request for later processing.

For example, a course-access service can commit an access grant before returning
success. It can also save the request to its own durable queue and return
success, taking responsibility for subsequent retries and recovery.

Returning success after starting only an in-memory background task can lose
work on a crash. Once WriteRelay records successful delivery, it cannot detect
later receiver failures and does not retry that delivery.

## Can the same event arrive more than once?

Yes. A receiver can complete the work but lose its success response, or
WriteRelay can crash before saving that success locally. Another attempt can
then arrive with the same `Idempotency-Key` for that delivery. Attempts do not
get fresh keys, and manual redrive preserves the key for the same delivery.

The receiver should use a unique key constraint and save the key and its
business changes in one database transaction. An already-committed key should
result in success without repeating the changes. See the
[certificate example](../README.md#handling-duplicate-webhooks) for the steps.

This protects changes within the receiver's database. Additional external
actions need their own duplicate handling.

## Which services can receive events?

WriteRelay currently supports stdout for development and HTTP webhook
destinations that accept its structured events. A sink is simply a configured
delivery destination.

HTTP support does not mean arbitrary third-party API compatibility. WriteRelay
sends the original event bytes; it does not translate them into provider-specific
operations or implement provider-specific authentication flows. There are no
built-in Stripe, QuickBooks, or SQS integrations.

To create an invoice through a provider API, for example, your integration
service could receive the WriteRelay event and make the appropriate API call.
Incoming provider notifications are handled by your application, which can then
record changes and emit new events in PostgreSQL.

## Could an HTTP wrapper forward events to SQS or another queue?

Yes. A wrapper can receive the event, send it to a queue, and return success
after the queue confirms acceptance. Transient send failures should produce a
retryable HTTP response so WriteRelay can try again. The wrapper should preserve
the event identity for downstream duplicate handling.

This means successful WriteRelay delivery represents acceptance by the queue.
The queue and its consumers own processing afterward. A lost acknowledgment can
still cause duplicate sends, so adding a wrapper does not remove the need for
duplicate handling. Direct queue support would require a new sink implementation
and configuration in WriteRelay.

## Can I clean up old events without losing retries or duplicate protection?

Use `spool prune` to remove payloads only after every associated delivery has
succeeded before your chosen cutoff. Pending work, retries, dead letters, and
events with no deliveries keep their payloads. Identity/digest and delivery
history remain, so identical replay is still recognized and conflicting content
still stops capture. Pruned payloads cannot be backfilled to newly added sinks.
The [retention guide](retention.md) explains previews, schema upgrades, and why
cleanup makes SQLite space reusable without automatically shrinking the file.
