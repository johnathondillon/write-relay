export default defineEventHandler(async (request) => {
  const body = await readBody(request);
  if (
    !["normal", "unavailable", "reject", "drop-response"].includes(body?.mode)
  ) {
    throw createError({ statusCode: 400, statusMessage: "Unknown demo mode." });
  }
  try {
    return await certificateRequest("/mode", { mode: body.mode });
  } catch {
    throw createError({
      statusCode: 503,
      statusMessage:
        "Certificate service is stopped. Start its container to use these controls.",
    });
  }
});
