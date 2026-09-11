import {
  emit,
  type TransactionClient,
  type WriteRelayEvent,
  withInbox,
  type InboxPool,
  type InboxResult,
} from "../dist/index.js";

interface CourseCompleted {
  completionId: string;
}
const event: WriteRelayEvent<CourseCompleted> = {
  id: "completion-1",
  source: "urn:test:lms",
  type: "course.completed",
  data: { completionId: "completion-1" },
};
declare const client: TransactionClient;
const result: Promise<void> = emit(client, event);
void result;
// @ts-expect-error IDs must be explicit so callers can preserve retry identity.
emit(client, { source: "urn:test:lms", type: "course.completed" });
// @ts-expect-error Envelope versions are fixed.
emit(client, { ...event, specversion: "2.0" });
// @ts-expect-error Dates must be serialized explicitly.
emit(client, { ...event, time: new Date() });
// @ts-expect-error Payload types are checked when a domain type is supplied.
emit<CourseCompleted>(client, { ...event, data: { completionId: 42 } });

declare const pool: InboxPool;
const receipt = { key: "a".repeat(64), body: new Uint8Array([123, 125]) };
const consumed: Promise<InboxResult<number>> = withInbox(
  pool,
  receipt,
  async (tx) => {
    await tx.query("INSERT INTO business(id) VALUES($1)", ["one"]);
    // @ts-expect-error The callback cannot release its managed client.
    tx.release();
    return 42;
  },
);
void consumed;
// @ts-expect-error Supply original bytes, not a JSON object or re-encoded string.
withInbox(pool, { key: receipt.key, body: "{}" }, async () => {});
// @ts-expect-error A transaction client is not a pool.
withInbox(client, receipt, async () => {});
