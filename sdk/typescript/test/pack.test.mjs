import assert from "node:assert/strict";
import test from "node:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("packed package imports in a standalone consumer without TypeScript or repository source", () => {
  const directory = mkdtempSync(join(tmpdir(), "writerelay-package-"));
  try {
    const [pack] = JSON.parse(
      execFileSync(
        "npm",
        ["pack", "--ignore-scripts", "--json", "--pack-destination", directory],
        { encoding: "utf8" },
      ),
    );
    assert.ok(pack.files.some((file) => file.path === "dist/index.js"));
    assert.ok(pack.files.some((file) => file.path === "dist/index.d.ts"));
    assert.ok(pack.files.some((file) => file.path === "LICENSE"));
    assert.ok(
      !pack.files.some(
        (file) => file.path.startsWith("src/") || file.path.startsWith("test/"),
      ),
    );
    writeFileSync(
      join(directory, "package.json"),
      '{"private":true,"type":"module"}',
    );
    execFileSync(
      "npm",
      [
        "install",
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        join(directory, pack.filename),
      ],
      { cwd: directory, stdio: "pipe" },
    );
    execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `
      import assert from 'node:assert/strict';
      import { emit } from '@writerelay/node';
      let calls = 0;
      await emit({ async query(sql, [payload]) {
        calls++;
        assert.equal(sql, 'SELECT writerelay.emit($1::jsonb)');
        assert.equal(JSON.parse(payload).id, 'pack-test');
      } }, { id: 'pack-test', source: 'urn:test', type: 'test' });
      assert.equal(calls, 1);
    `,
      ],
      { cwd: directory, stdio: "pipe" },
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
