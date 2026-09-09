import assert from "node:assert/strict";
import test from "node:test";
import { emit } from "@writerelay/node";

const event = () => ({
  id: "completion-1",
  source: "urn:test:lms",
  type: "course.completed",
});
function recorder() {
  return {
    calls: [],
    async query(...args) {
      this.calls.push(args);
      return { rows: [{ emit: "0/1234" }] };
    },
  };
}

test("uses one parameterized statement on the supplied client and leaves transaction ownership to caller", async () => {
  const client = recorder();
  const input = Object.freeze({
    ...event(),
    data: { title: "'); COMMIT; --", nested: [null, true, 2, "🎓"] },
  });
  assert.equal(await emit(client, input), undefined);
  assert.equal(client.calls.length, 1);
  const [sql, values] = client.calls[0];
  assert.equal(sql, "SELECT writerelay.emit($1::jsonb)");
  assert.equal(values.length, 1);
  assert.deepEqual(JSON.parse(values[0]), {
    ...input,
    specversion: "1.0",
    datacontenttype: "application/json",
  });
  assert.equal(input.specversion, undefined);
});

test("minimal envelopes and explicit optional metadata", async () => {
  const client = recorder();
  await emit(client, event());
  assert.deepEqual(JSON.parse(client.calls[0][1][0]), {
    ...event(),
    specversion: "1.0",
  });
  const input = {
    ...event(),
    specversion: "1.0",
    subject: "",
    time: "2024-02-29T23:59:59.123456789+05:30",
    data: null,
    datacontenttype: "application/example+json",
  };
  await emit(client, input);
  assert.deepEqual(JSON.parse(client.calls[1][1][0]), input);
});

test("repeated calls preserve caller identity and payload bytes", async () => {
  const client = recorder();
  const input = { ...event(), data: { completionId: "completion-1" } };
  await emit(client, input);
  await emit(client, input);
  assert.deepEqual(client.calls[0], client.calls[1]);
});

test("awaits the database and propagates its error without retrying or committing", async () => {
  const failure = new Error("database rejected event");
  let rejectQuery;
  let calls = 0;
  const pending = emit(
    {
      query() {
        calls++;
        return new Promise((_, reject) => {
          rejectQuery = reject;
        });
      },
    },
    event(),
  );
  rejectQuery(failure);
  await assert.rejects(pending, (error) => error === failure);
  assert.equal(calls, 1);
});

const invalidEnvelopes = [
  null,
  [],
  "event",
  ...["id", "source", "type"].flatMap((key) =>
    [undefined, null, "", 42].map((value) => ({ ...event(), [key]: value })),
  ),
  { ...event(), specversion: "2.0" },
  { ...event(), specversion: null },
  { ...event(), subject: null },
  { ...event(), datacontenttype: "" },
  { ...event(), datacontenttype: false },
  { ...event(), typo: true },
];
test("rejects malformed envelopes before issuing SQL", async () => {
  const client = recorder();
  for (const input of invalidEnvelopes)
    await assert.rejects(emit(client, input), TypeError);
  assert.equal(client.calls.length, 0);
});

test("validates calendar dates, timezone, and RFC 3339 syntax", async () => {
  const client = recorder();
  for (const time of [
    "2026-02-29T00:00:00Z",
    "1900-02-29T00:00:00Z",
    "2026-04-31T00:00:00Z",
    "2026-01-00T00:00:00Z",
    "2026-13-01T00:00:00Z",
    "2026-09-08",
    "2026-09-08T00:00:00",
    "2026-09-08T24:00:00Z",
    "2026-09-08T00:60:00Z",
    "2026-09-08T00:00:60Z",
    "2026-09-08T00:00:00+24:00",
    "2026-09-08T00:00:00+00:60",
    "",
    null,
    1,
  ]) {
    await assert.rejects(emit(client, { ...event(), time }), /RFC 3339/);
  }
  assert.equal(client.calls.length, 0);
  for (const time of [
    "2000-02-29T00:00:00Z",
    "2026-09-08T00:00:00-05:00",
    "2026-09-08T00:00:00.123Z",
  ]) {
    await emit(client, { ...event(), time });
  }
});

test("rejects lossy JSON data, cycles, and strings PostgreSQL cannot store", async () => {
  const cyclic = {};
  cyclic.self = cyclic;
  const client = recorder();
  for (const data of [
    NaN,
    Infinity,
    1n,
    () => {},
    Symbol(),
    { missing: undefined },
    [undefined],
    Array(1),
    new Date(),
    new Map(),
    {
      toJSON() {
        return {};
      },
    },
    cyclic,
    "\u0000",
    "\uD800",
    "\uDC00",
    { "\u0000": true },
  ]) {
    await assert.rejects(emit(client, { ...event(), data }), TypeError);
  }
  assert.equal(client.calls.length, 0);
});

test("shared references are allowed; unusual JSON keys survive serialization", async () => {
  const shared = { value: "🎓" };
  const data = JSON.parse('{"__proto__":{"ok":true},"constructor":"course"}');
  data.items = [shared, shared];
  const client = recorder();
  await emit(client, { ...event(), data });
  assert.deepEqual(JSON.parse(client.calls[0][1][0]).data, data);
});
