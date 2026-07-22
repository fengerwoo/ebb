import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { parseMetaCommand, runCatalogCommand } from "../scripts/lib/catalog.mjs";
import { renderCsv, renderJson, writeResult } from "../scripts/lib/output.mjs";
import { parseArgs } from "../scripts/ebb-query-data.mjs";

const config = {
  databases: {
    operations: { tables: { metrics: {}, events: {} } },
    analytics: { tables: { snapshots: {}, events: {} } },
  },
  query: { defaultDatabase: "analytics" },
};

test("metadata parser handles supported MySQL-style statements", () => {
  assert.deepEqual(parseMetaCommand("SHOW DATABASES;"), { kind: "show-databases" });
  assert.deepEqual(parseMetaCommand("show tables from operations"), {
    kind: "show-tables",
    database: "operations",
  });
  assert.deepEqual(parseMetaCommand("DESC events;", "analytics"), {
    kind: "describe-table",
    database: "analytics",
    table: "events",
  });
  assert.deepEqual(parseMetaCommand("SHOW COLUMNS FROM operations.metrics"), {
    kind: "describe-table",
    database: "operations",
    table: "metrics",
  });
  assert.throws(() => parseMetaCommand("SHOW CREATE TABLE events"), /unsupported metadata/);
});

test("catalog results are stable and sorted", () => {
  const databases = runCatalogCommand(config, { kind: "show-databases" });
  assert.deepEqual(databases.rows, [["analytics"], ["operations"]]);

  const tables = runCatalogCommand(config, {
    kind: "show-tables",
    database: "analytics",
  });
  assert.deepEqual(tables.rows, [["events"], ["snapshots"]]);
});

test("JSON output uses row objects and preserves metadata", () => {
  const rendered = JSON.parse(
    renderJson({
      columns: [
        { name: "id", type: "BIGINT" },
        { name: "amount", type: "DECIMAL(10,2)" },
      ],
      rows: [["9007199254740993", "12.30"]],
      rowCount: 1,
      truncated: false,
      elapsedMs: 3,
    }),
  );
  assert.deepEqual(rendered.rows, [{ id: "9007199254740993", amount: "12.30" }]);
  assert.equal(rendered.columns[0].type, "BIGINT");
});

test("CSV output quotes commas, quotes, newlines, objects, and nulls", () => {
  const rendered = renderCsv({
    columns: [
      { name: "text", type: "VARCHAR" },
      { name: "nested", type: "STRUCT" },
      { name: "empty", type: "VARCHAR" },
    ],
    rows: [['a,"b"\nline', { ok: true }, null]],
  });
  assert.equal(rendered, 'text,nested,empty\n"a,""b""\nline","{""ok"":true}",\n');
});

test("CLI arguments support CSV files and validate incompatible options", () => {
  const args = parseArgs([
    "--project-root",
    "/tmp/project",
    "--sql",
    "SHOW DATABASES;",
    "--format=csv",
    "--output",
    "result.csv",
    "--no-header",
  ]);
  assert.equal(args.format, "csv");
  assert.equal(args.header, false);
  assert.equal(args.outputPath, "result.csv");
  assert.throws(
    () => parseArgs(["--sql", "SELECT 1", "--no-header"]),
    /only valid with --format csv/,
  );
});

test("file output stays inside the project and never overwrites", () => {
  const projectRoot = fs.mkdtempSync(path.join(os.tmpdir(), "ebb-query-output-"));
  const result = { rowCount: 1, truncated: false, elapsedMs: 1 };
  const stdout = { write() {} };

  const created = writeResult({
    content: "id\n1\n",
    outputPath: "result.csv",
    projectRoot,
    format: "csv",
    result,
    stdout,
  });
  assert.equal(fs.readFileSync(created, "utf8"), "id\n1\n");
  assert.throws(
    () =>
      writeResult({
        content: "replace",
        outputPath: "result.csv",
        projectRoot,
        format: "csv",
        result,
        stdout,
      }),
    { code: "OUTPUT_WRITE_FAILED" },
  );
  assert.throws(
    () =>
      writeResult({
        content: "escape",
        outputPath: "../escape.csv",
        projectRoot,
        format: "csv",
        result,
        stdout,
      }),
    { code: "OUTPUT_PATH_INVALID" },
  );
  assert.throws(
    () =>
      writeResult({
        content: "absolute",
        outputPath: path.join(projectRoot, "absolute.csv"),
        projectRoot,
        format: "csv",
        result,
        stdout,
      }),
    { code: "OUTPUT_PATH_INVALID" },
  );
});
