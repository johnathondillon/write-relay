/** PostgreSQL result shape used by the receiver helper (compatible with pg). */
export interface InboxQueryResult {
  command: string;
  rowCount: number | null;
  rows: Record<string, unknown>[];
}

/** All business SQL in the callback must use this transaction. */
export interface InboxTransaction {
  query(text: string, values?: unknown[]): Promise<InboxQueryResult>;
}

export interface InboxClient extends InboxTransaction {
  release(destroy?: boolean): void;
}

/** A pool that checks out one dedicated PostgreSQL client per invocation. */
export interface InboxPool {
  connect(): Promise<InboxClient>;
}

export interface InboxTable {
  /** Defaults to public. Use a separate table per independent consumer. */
  schema?: string;
  /** Defaults to writerelay_inbox. */
  table?: string;
}

export interface InboxDelivery extends InboxTable {
  /** The unmodified WriteRelay Idempotency-Key header (64 lowercase hex digits). */
  key: string;
  /** Original request bytes, before JSON parsing/re-encoding. Buffer is accepted. */
  body: Uint8Array;
}

export type InboxResult<T> =
  | { status: "processed"; value: T }
  | { status: "duplicate" };

/** An existing delivery key was reused with different request bytes. */
export class InboxConflictError extends Error {
  constructor() {
    super("WriteRelay inbox key was reused with different content");
    this.name = "InboxConflictError";
  }
}

/** A transaction or inbox result did not satisfy the processing contract. */
export class InboxTransactionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InboxTransactionError";
  }
}

function tableName(options: InboxTable): string {
  const schema = options.schema ?? "public";
  const table = options.table ?? "writerelay_inbox";
  for (const value of [schema, table]) {
    if (
      typeof value !== "string" ||
      !/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(value)
    ) {
      throw new TypeError(
        "WriteRelay inbox identifiers must be ASCII SQL identifiers of at most 63 characters",
      );
    }
  }
  return `"${schema}"."${table}"`;
}

/** Return DDL for an application-owned migration. Never run per request. */
export function inboxTableSQL(options: InboxTable = {}): string {
  return `CREATE TABLE ${tableName(options)} (
  idempotency_key text PRIMARY KEY CHECK (idempotency_key ~ '^[a-f0-9]{64}$'),
  payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
  received_at timestamptz NOT NULL DEFAULT now()
)`;
}

/**
 * Commit the inbox key and the callback's database writes together. Authenticate
 * and validate the request first. The callback must only use the supplied
 * transaction, must not manage it, and must not perform external side effects.
 * Return HTTP success only after this promise resolves. Retain inbox records
 * for as long as duplicates/redrives can arrive. No automatic retries occur.
 */
export async function withInbox<T>(
  pool: InboxPool,
  delivery: InboxDelivery,
  handle: (transaction: InboxTransaction) => Promise<T>,
): Promise<InboxResult<T>> {
  if (
    delivery === null ||
    typeof delivery !== "object" ||
    typeof delivery.key !== "string" ||
    !/^[a-f0-9]{64}$/.test(delivery.key)
  ) {
    throw new TypeError("WriteRelay inbox key must be 64 lowercase hex digits");
  }
  if (
    !(delivery.body instanceof Uint8Array) ||
    delivery.body.byteLength === 0
  ) {
    throw new TypeError(
      "WriteRelay inbox body must contain the original request bytes",
    );
  }
  if (typeof handle !== "function") {
    throw new TypeError("WriteRelay inbox handler must be a function");
  }
  const table = tableName(delivery);
  const key = delivery.key;
  // Snapshot before the first await; caller mutations must not change the digest.
  const bytes = Uint8Array.from(delivery.body);
  const digest = Array.from(
    new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)),
    (byte) => byte.toString(16).padStart(2, "0"),
  ).join("");
  const client = await pool.connect();
  let discard = false;
  let committing = false;
  try {
    // The SELECT after a conflicting INSERT needs a fresh statement snapshot.
    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    const claim = await client.query(
      `INSERT INTO ${table} (idempotency_key, payload_sha256) VALUES ($1, $2) ON CONFLICT (idempotency_key) DO NOTHING RETURNING idempotency_key`,
      [key, digest],
    );
    let result: InboxResult<T>;
    if (claim.rowCount === 1) {
      let active = true;
      const transaction: InboxTransaction = {
        query(text, values) {
          if (!active)
            return Promise.reject(
              new InboxTransactionError(
                "WriteRelay inbox callback has finished",
              ),
            );
          return client.query(text, values);
        },
      };
      try {
        result = { status: "processed", value: await handle(transaction) };
      } finally {
        active = false;
      }
    } else if (claim.rowCount === 0) {
      const existing = await client.query(
        `SELECT payload_sha256 FROM ${table} WHERE idempotency_key = $1 FOR SHARE`,
        [key],
      );
      if (existing.rows.length !== 1) {
        throw new InboxTransactionError(
          "WriteRelay inbox claim disappeared; processing was not attempted",
        );
      }
      if (existing.rows[0].payload_sha256 !== digest)
        throw new InboxConflictError();
      result = { status: "duplicate" };
    } else {
      throw new InboxTransactionError(
        "WriteRelay inbox insert returned an unexpected result",
      );
    }
    committing = true;
    const committed = await client.query("COMMIT");
    // PostgreSQL returns ROLLBACK if a callback swallowed an error that aborted
    // its transaction. Do not report that as successful processing.
    if (committed.command !== "COMMIT") {
      throw new InboxTransactionError(
        "WriteRelay inbox transaction did not commit",
      );
    }
    return result;
  } catch (error) {
    // A commit error may have an unknown outcome; discard that connection and
    // let the caller retry the same key/content, never the callback alone.
    discard = committing;
    try {
      await client.query("ROLLBACK");
    } catch {
      discard = true;
    }
    throw error;
  } finally {
    client.release(discard);
  }
}
