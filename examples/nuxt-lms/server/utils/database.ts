import pg from "pg";

export const database = new pg.Pool({
  connectionString: process.env.LMS_DATABASE_URL,
  max: 5,
  connectionTimeoutMillis: 3000,
  statement_timeout: 5000,
});
database.on("error", () => console.error("LMS database connection failed"));
