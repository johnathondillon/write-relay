export default defineEventHandler(async () => {
  const completions = await database.query(
    "SELECT * FROM completions ORDER BY completed_at DESC, id DESC LIMIT 50",
  );
  let receiver: unknown = null;
  try {
    receiver = await certificateRequest("/status");
  } catch {
    // Receiver unavailability must not hide committed LMS records.
  }
  return { completions: completions.rows, receiver };
});
