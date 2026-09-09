import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { createHash, randomUUID } from "node:crypto";
import pg from "pg";

const database = new pg.Pool({
  connectionString: process.env.CERTIFICATE_DATABASE_URL,
  max: 5,
  connectionTimeoutMillis: 3000,
  statement_timeout: 5000,
});
database.on("error", () =>
  console.error("Certificate database connection failed"),
);
if (!process.env.EXAMPLE_TOKEN) throw new Error("EXAMPLE_TOKEN is required");

// Failure controls exist only in this local example, never in writerelayd.
const modes = ["normal", "unavailable", "reject", "drop-response"] as const;
let mode: (typeof modes)[number] = "normal";

function respond(response: ServerResponse, status: number, body: unknown) {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

async function readBytes(request: IncomingMessage) {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 256 * 1024) throw new Error("Request too large");
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

async function handle(request: IncomingMessage, response: ServerResponse) {
  if (request.url === "/health" && request.method === "GET") {
    await database.query("SELECT 1");
    return respond(response, 200, { ok: true });
  }
  if (request.headers.authorization !== `Bearer ${process.env.EXAMPLE_TOKEN}`) {
    return respond(response, 401, { error: "Unauthorized" });
  }
  if (request.url === "/status" && request.method === "GET") {
    const certificates = await database.query(
      "SELECT * FROM certificates ORDER BY issued_at DESC LIMIT 100",
    );
    const receipts = await database.query(
      "SELECT * FROM receipts ORDER BY sequence DESC LIMIT 100",
    );
    return respond(response, 200, {
      mode,
      certificates: certificates.rows,
      receipts: receipts.rows,
    });
  }
  if (request.method !== "POST")
    return respond(response, 404, { error: "Not found" });

  let raw: Buffer;
  let body: any;
  try {
    raw = await readBytes(request);
    body = JSON.parse(raw.toString("utf8"));
  } catch {
    return respond(response, 400, { error: "Invalid or oversized JSON" });
  }
  if (request.url === "/mode") {
    if (!modes.includes(body?.mode))
      return respond(response, 400, { error: "Invalid mode" });
    mode = body.mode;
    return respond(response, 200, { mode });
  }
  if (request.url !== "/events")
    return respond(response, 404, { error: "Not found" });

  const key = request.headers["idempotency-key"];
  const data = body?.data;
  if (
    typeof key !== "string" ||
    !/^[a-f0-9]{64}$/.test(key) ||
    body?.specversion !== "1.0" ||
    body?.source !== "urn:writerelay:example:lms" ||
    body?.type !== "course.completed" ||
    body?.id !== data?.completionId ||
    typeof data?.completionId !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      data.completionId,
    ) ||
    typeof data?.learner !== "string" ||
    !data.learner ||
    data.learner.length > 80 ||
    typeof data?.courseTitle !== "string" ||
    !data.courseTitle ||
    data.courseTitle.length > 200
  ) {
    return respond(response, 422, { error: "Invalid course.completed event" });
  }

  if (mode === "unavailable" || mode === "reject") {
    const outcome = mode === "unavailable" ? "unavailable" : "rejected";
    await database.query(
      "INSERT INTO receipts (completion_id, idempotency_key, outcome) VALUES ($1, $2, $3)",
      [data.completionId, key, outcome],
    );
    return respond(response, mode === "unavailable" ? 503 : 422, {
      error: `Demo: ${outcome}`,
    });
  }

  const dropResponse = mode === "drop-response";
  if (dropResponse) mode = "normal";
  const digest = createHash("sha256").update(raw).digest("hex");
  const client = await database.connect();
  let outcome = "issued";
  try {
    await client.query("BEGIN");
    // ON CONFLICT waits for a concurrent insert to commit or roll back.
    const inserted = await client.query(
      "INSERT INTO inbox (idempotency_key, payload_sha256) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING idempotency_key",
      [key, digest],
    );
    if (inserted.rowCount === 0) {
      const previous = await client.query(
        "SELECT payload_sha256 FROM inbox WHERE idempotency_key = $1",
        [key],
      );
      if (previous.rows[0].payload_sha256 !== digest) {
        await client.query("ROLLBACK");
        return respond(response, 409, {
          error: "Same key with different content",
        });
      }
      outcome = "duplicate";
    } else {
      await client.query(
        `INSERT INTO certificates (completion_id, certificate_id, learner, course_title)
         VALUES ($1, $2, $3, $4)`,
        [data.completionId, randomUUID(), data.learner, data.courseTitle],
      );
    }
    await client.query(
      "INSERT INTO receipts (completion_id, idempotency_key, outcome) VALUES ($1, $2, $3)",
      [data.completionId, key, dropResponse ? "response-lost" : outcome],
    );
    // The key and certificate become durable together, BEFORE success is sent.
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
  if (dropResponse) {
    response.destroy();
    return;
  }
  respond(response, 200, { outcome });
}

const server = createServer((request, response) => {
  void handle(request, response).catch(() => {
    console.error("Certificate request failed; caller may retry");
    if (!response.headersSent && !response.destroyed)
      respond(response, 503, { error: "Temporary receiver failure" });
    else response.destroy();
  });
});
server.requestTimeout = 10_000;
server.listen(3001, "0.0.0.0", () =>
  console.log("Certificate service listening on :3001"),
);
process.on("SIGTERM", () => {
  server.close(() => {
    void database.end();
  });
});
