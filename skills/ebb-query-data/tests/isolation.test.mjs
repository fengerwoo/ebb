import assert from "node:assert/strict";
import test from "node:test";

import { runProcessWithTimeout } from "../scripts/lib/isolation.mjs";

function sink() {
  return { write() {} };
}

test("isolated processes are forcibly bounded by a wall-clock timeout", async () => {
  const started = Date.now();
  const result = await runProcessWithTimeout(
    process.execPath,
    ["-e", "setInterval(() => {}, 1000)"],
    {
      timeoutMs: 30,
      killGraceMs: 30,
      stdout: sink(),
      stderr: sink(),
    },
  );
  assert.equal(result.timedOut, true);
  assert.ok(Date.now() - started < 1_000);
});
