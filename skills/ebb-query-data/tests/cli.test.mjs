import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";

const skillRoot = path.resolve(import.meta.dirname, "..");
const cli = path.join(skillRoot, "scripts", "ebb-query-data.mjs");
const example = path.join(skillRoot, "ebb-query-data.example.yaml");

function makeProject() {
  const project = fs.mkdtempSync(path.join(os.tmpdir(), "ebb-query-cli-"));
  fs.copyFileSync(example, path.join(project, "ebb-query-data.yaml"));
  return project;
}

function invoke(args) {
  return spawnSync(process.execPath, [cli, ...args], {
    encoding: "utf8",
    env: { ...process.env, NO_COLOR: "1" },
  });
}

test("CLI discovers project config and emits JSON metadata", () => {
  const project = makeProject();
  const result = invoke(["--project-root", project, "--sql", "SHOW DATABASES;"]);
  assert.equal(result.status, 0, result.stderr);
  const body = JSON.parse(result.stdout);
  assert.deepEqual(
    body.rows.map((row) => row.database),
    ["analytics", "operations"],
  );
});

test("CLI writes project-relative CSV and returns a JSON summary", () => {
  const project = makeProject();
  const result = invoke([
    "--project-root",
    project,
    "--format",
    "csv",
    "--output",
    "tables.csv",
    "--sql",
    "SHOW TABLES FROM analytics;",
  ]);
  assert.equal(result.status, 0, result.stderr);
  const summary = JSON.parse(result.stdout);
  assert.equal(summary.output, path.join(project, "tables.csv"));
  assert.equal(fs.readFileSync(summary.output, "utf8"), "table\nevents\nmetrics\n");
});

test("CLI sends structured failures to stderr without stdout noise", () => {
  const project = makeProject();
  const result = invoke([
    "--project-root",
    project,
    "--sql",
    "SHOW CREATE TABLE events;",
  ]);
  assert.equal(result.status, 3);
  assert.equal(result.stdout, "");
  assert.deepEqual(JSON.parse(result.stderr), {
    error: {
      code: "SQL_UNSUPPORTED",
      message: "unsupported metadata statement",
    },
  });
});
