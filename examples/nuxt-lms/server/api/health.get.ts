export default defineEventHandler(async () => {
  await database.query("SELECT 1");
  return { ok: true };
});
