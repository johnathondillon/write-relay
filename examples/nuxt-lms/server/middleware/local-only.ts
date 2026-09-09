export default defineEventHandler((request) => {
  // No authentication in this disposable demo. Reject browser cross-origin writes.
  if (request.method !== "POST") return;
  const origin = getHeader(request, "origin");
  const host = getHeader(request, "host");
  if (origin && new URL(origin).host !== host) {
    throw createError({
      statusCode: 403,
      statusMessage: "Cross-origin writes are not allowed.",
    });
  }
  if (!getHeader(request, "content-type")?.startsWith("application/json")) {
    throw createError({
      statusCode: 415,
      statusMessage: "Use application/json.",
    });
  }
});
