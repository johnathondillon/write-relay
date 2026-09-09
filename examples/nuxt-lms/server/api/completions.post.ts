import { courses } from "#shared/courses";
import { emit } from "@writerelay/node";

export default defineEventHandler(async (request) => {
  const body = await readBody(request);
  const course = courses.find((course) => course.id === body?.courseId);
  if (
    !course ||
    typeof body?.id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      body.id,
    ) ||
    typeof body?.learner !== "string" ||
    !body.learner.trim() ||
    body.learner.length > 80 ||
    (body.rollback !== undefined && typeof body.rollback !== "boolean")
  ) {
    throw createError({
      statusCode: 400,
      statusMessage: "Provide a request UUID, learner name, and valid course.",
    });
  }

  // Every statement must use this same checked-out connection.
  const client = await database.connect();
  try {
    await client.query("BEGIN");
    const inserted = await client.query(
      `INSERT INTO completions (id, learner, course_id, course_title)
       VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING RETURNING *`,
      [body.id, body.learner.trim(), course.id, course.title],
    );
    if (inserted.rowCount === 0) {
      const existing = await client.query(
        "SELECT * FROM completions WHERE id = $1",
        [body.id],
      );
      if (
        existing.rows[0].learner !== body.learner.trim() ||
        existing.rows[0].course_id !== course.id
      ) {
        throw createError({
          statusCode: 409,
          statusMessage:
            "This request ID already belongs to another completion.",
        });
      }
      await client.query("COMMIT");
      return { completion: existing.rows[0], replay: true, rolledBack: false };
    }

    const completion = inserted.rows[0];
    // Use the SAME client as the INSERT; the application owns the transaction.
    await emit(client, {
      id: completion.id,
      source: "urn:writerelay:example:lms",
      type: "course.completed",
      subject: completion.id,
      time: completion.completed_at.toISOString(),
      data: {
        completionId: completion.id,
        learner: completion.learner,
        courseId: course.id,
        courseTitle: course.title,
      },
    });
    // Example-only rollback demonstration, after BOTH statements have executed.
    await client.query(body.rollback ? "ROLLBACK" : "COMMIT");
    return { completion, rolledBack: body.rollback === true, replay: false };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
});
