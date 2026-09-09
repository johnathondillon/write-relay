import {
  emit,
  type TransactionClient,
  type WriteRelayEvent,
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
