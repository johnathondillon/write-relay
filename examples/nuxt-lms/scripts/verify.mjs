import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

const base = process.env.LMS_URL || "http://127.0.0.1:3000";
async function request(path, body) {
  const response = await fetch(`${base}/api/${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(
    response.ok,
    true,
    `${path}: ${response.status} ${await response.clone().text()}`,
  );
  return response.json();
}
async function until(description, predicate, timeout = 25_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const state = await request("status");
    if (predicate(state)) return state;
    await delay(250);
  }
  throw new Error(`Timed out: ${description}`);
}
const hasCertificate = (state, id) =>
  state.receiver?.certificates.some((row) => row.completion_id === id);
const event = () => ({
  id: randomUUID(),
  learner: "Example verification",
  courseId: "reliable-events",
});

try {
  await request("receiver", { mode: "normal" });
  const normal = event();
  // Concurrent HTTP retries of one producer request must emit only one event.
  const responses = await Promise.all(
    Array.from({ length: 4 }, () => request("completions", normal)),
  );
  assert.equal(responses.filter((row) => !row.replay).length, 1);
  let state = await until("normal delivery", (state) =>
    hasCertificate(state, normal.id),
  );
  assert.equal(
    state.completions.filter((row) => row.id === normal.id).length,
    1,
  );
  assert.equal(
    state.receiver.certificates.filter((row) => row.completion_id === normal.id)
      .length,
    1,
  );
  console.log(
    "PASS: concurrent producer requests create one completion and certificate",
  );

  const rollback = { ...event(), rollback: true };
  assert.equal((await request("completions", rollback)).rolledBack, true);
  const marker = event();
  await request("completions", marker);
  state = await until("marker after rollback", (state) =>
    hasCertificate(state, marker.id),
  );
  assert.equal(
    state.completions.some((row) => row.id === rollback.id),
    false,
  );
  assert.equal(
    state.receiver.receipts.some((row) => row.completion_id === rollback.id),
    false,
  );
  assert.equal(hasCertificate(state, rollback.id), false);
  console.log(
    "PASS: rolled-back completion and event absent after later committed event is delivered",
  );

  await request("receiver", { mode: "unavailable" });
  const outage = event();
  await request("completions", outage);
  state = await until("retryable failure", (state) =>
    state.receiver?.receipts.some(
      (row) => row.completion_id === outage.id && row.outcome === "unavailable",
    ),
  );
  assert.equal(
    state.completions.some((row) => row.id === outage.id),
    true,
  );
  assert.equal(hasCertificate(state, outage.id), false);
  await request("receiver", { mode: "normal" });
  await until("automatic recovery", (state) =>
    hasCertificate(state, outage.id),
  );
  console.log(
    "PASS: completion survives receiver failure; delivery retries automatically",
  );

  await request("receiver", { mode: "drop-response" });
  const duplicate = event();
  await request("completions", duplicate);
  state = await until("lost response and deduplicated retry", (state) =>
    state.receiver?.receipts.some(
      (row) =>
        row.completion_id === duplicate.id && row.outcome === "duplicate",
    ),
  );
  const receipts = state.receiver.receipts.filter(
    (row) => row.completion_id === duplicate.id,
  );
  assert.ok(receipts.some((row) => row.outcome === "response-lost"));
  assert.equal(new Set(receipts.map((row) => row.idempotency_key)).size, 1);
  assert.equal(
    state.receiver.certificates.filter(
      (row) => row.completion_id === duplicate.id,
    ).length,
    1,
  );
  console.log(
    "PASS: response lost after commit; same key retried; one certificate",
  );
} finally {
  await request("receiver", { mode: "normal" });
}
