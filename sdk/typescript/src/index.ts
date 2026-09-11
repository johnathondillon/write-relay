export {
  withInbox,
  inboxTableSQL,
  InboxConflictError,
  InboxTransactionError,
  type InboxPool,
  type InboxClient,
  type InboxTransaction,
  type InboxQueryResult,
  type InboxDelivery,
  type InboxTable,
  type InboxResult,
} from "./inbox.js";

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

/** The JSON event envelope supported by this first version of the SDK. */
export interface WriteRelayEvent<T = JsonValue> {
  id: string;
  source: string;
  type: string;
  specversion?: "1.0";
  subject?: string;
  time?: string;
  datacontenttype?: string;
  data?: T;
}

/** A checked-out PostgreSQL client; all transaction statements must use it. */
export interface TransactionClient {
  query(text: string, values: [string]): Promise<unknown>;
}

/**
 * Validate and emit an event on the supplied client. Await this before COMMIT.
 * This does not begin, commit, retry, or verify the existence of a transaction.
 * Never pass a pool: use the same checked-out client as the business writes.
 * Resolution means the SQL statement succeeded, not that delivery occurred.
 */
export async function emit<T>(
  client: TransactionClient,
  event: WriteRelayEvent<T>,
): Promise<void> {
  if (event === null || typeof event !== "object" || Array.isArray(event)) {
    throw new TypeError("WriteRelay event must be an object");
  }
  const allowed = new Set([
    "specversion",
    "id",
    "source",
    "type",
    "subject",
    "time",
    "datacontenttype",
    "data",
  ]);
  for (const key of Object.keys(event)) {
    if (!allowed.has(key))
      throw new TypeError("Unsupported WriteRelay event attribute");
  }
  for (const key of ["id", "source", "type"] as const) {
    if (typeof event[key] !== "string" || event[key] === "") {
      throw new TypeError(`WriteRelay ${key} must be a non-empty string`);
    }
  }
  if (event.specversion !== undefined && event.specversion !== "1.0") {
    throw new TypeError("WriteRelay specversion must be 1.0");
  }
  if (event.subject !== undefined && typeof event.subject !== "string") {
    throw new TypeError("WriteRelay subject must be a string");
  }
  if (
    event.datacontenttype !== undefined &&
    (typeof event.datacontenttype !== "string" || event.datacontenttype === "")
  ) {
    throw new TypeError(
      "WriteRelay datacontenttype must be a non-empty string",
    );
  }
  if (event.time !== undefined && !isTimestamp(event.time)) {
    throw new TypeError("WriteRelay time must be an RFC 3339 timestamp");
  }

  const envelope = {
    specversion: "1.0",
    id: event.id,
    source: event.source,
    type: event.type,
    ...(event.subject === undefined ? {} : { subject: event.subject }),
    ...(event.time === undefined ? {} : { time: event.time }),
    ...(event.data === undefined ? {} : { data: event.data }),
    ...(event.datacontenttype === undefined
      ? event.data === undefined
        ? {}
        : { datacontenttype: "application/json" }
      : { datacontenttype: event.datacontenttype }),
  };
  // Copy JSON values without invoking toJSON or silently dropping unsupported data.
  const payload = JSON.stringify(jsonValue(envelope, new Set()));
  await client.query("SELECT writerelay.emit($1::jsonb)", [payload]);
}

function isTimestamp(value: unknown): boolean {
  if (typeof value !== "string") return false;
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|([+-])(\d{2}):(\d{2}))$/.exec(
      value,
    );
  if (!match) return false;
  const [, year, month, day, hour, minute, second, , offsetHour, offsetMinute] =
    match;
  const y = Number(year),
    m = Number(month),
    d = Number(day);
  const leap = y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return (
    m >= 1 &&
    m <= 12 &&
    d >= 1 &&
    d <= days[m - 1] &&
    Number(hour) < 24 &&
    Number(minute) < 60 &&
    Number(second) < 60 &&
    (offsetHour === undefined ||
      (Number(offsetHour) < 24 && Number(offsetMinute) < 60))
  );
}

function jsonValue(value: unknown, ancestors: Set<object>): JsonValue {
  if (value === null || typeof value === "boolean") return value;
  if (typeof value === "string") {
    // PostgreSQL jsonb rejects NUL and unpaired UTF-16 surrogates.
    if (
      /\u0000|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(
        value,
      )
    ) {
      throw new TypeError(
        "WriteRelay strings must be valid Unicode without NUL",
      );
    }
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "object") {
    throw new TypeError("WriteRelay data must contain only JSON values");
  }
  if (ancestors.has(value))
    throw new TypeError("WriteRelay data must not contain cycles");
  if (
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) !== Object.prototype &&
    Object.getPrototypeOf(value) !== null
  ) {
    throw new TypeError(
      "WriteRelay data must use plain objects and arrays; serialize classes explicitly",
    );
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return Array.from(value, (item) => jsonValue(item, ancestors));
    }
    const result: { [key: string]: JsonValue } = Object.create(null);
    for (const [key, item] of Object.entries(value)) {
      jsonValue(key, ancestors);
      result[key] = jsonValue(item, ancestors);
    }
    return result;
  } finally {
    ancestors.delete(value);
  }
}
