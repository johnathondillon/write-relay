export async function certificateRequest(path: string, body?: unknown) {
  return await $fetch(`${process.env.CERTIFICATE_URL}${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: { authorization: `Bearer ${process.env.EXAMPLE_TOKEN}` },
    ...(body === undefined ? {} : { body: body as Record<string, unknown> }),
    timeout: 2000,
    retry: 0,
  });
}
