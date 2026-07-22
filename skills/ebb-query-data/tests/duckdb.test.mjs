import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { DuckDBConnection } from "@duckdb/node-api";

import {
  executeReadOnlyQuery,
  partitionGlobExpression,
  planReadOnlyQuery,
  validateSelectStatement,
} from "../scripts/lib/duckdb.mjs";

const queryConfig = {
  databases: {
    alpha: {
      tables: {
        events: { name: "events" },
        metrics: { name: "metrics" },
      },
    },
    beta: {
      tables: {
        events: { name: "events" },
      },
    },
  },
};

test("read-only execution preserves types and enforces row truncation", async () => {
  const connection = await DuckDBConnection.create();
  try {
    const result = await executeReadOnlyQuery(
      connection,
      "SELECT i::BIGINT AS id, (i + 0.25)::DECIMAL(10,2) AS amount FROM range(5) t(i)",
      2,
      5,
    );
    assert.deepEqual(result.columns, [
      { name: "id", type: "BIGINT" },
      { name: "amount", type: "DECIMAL(10,2)" },
    ]);
    assert.deepEqual(result.rows, [
      ["0", "0.25"],
      ["1", "1.25"],
    ]);
    assert.equal(result.rowCount, 2);
    assert.equal(result.truncated, true);
  } finally {
    connection.closeSync();
  }
});

test("DuckDB parser rejects writes and multiple statements", async () => {
  const connection = await DuckDBConnection.create();
  try {
    await assert.rejects(validateSelectStatement(connection, "CREATE TABLE x(i int)"), {
      code: "SQL_REJECTED",
    });
    await assert.rejects(validateSelectStatement(connection, "SELECT 1; SELECT 2"), {
      code: "SQL_REJECTED",
    });
    await assert.rejects(
      validateSelectStatement(connection, "COPY (SELECT 1) TO '/tmp/ebb-query-test.csv'"),
      { code: "SQL_REJECTED" },
    );
    await assert.doesNotReject(
      validateSelectStatement(connection, "WITH x AS (SELECT 1 AS n) SELECT * FROM x"),
    );
  } finally {
    connection.closeSync();
  }
});

test("duplicate SQL column names are deterministically deduplicated", async () => {
  const connection = await DuckDBConnection.create();
  try {
    const result = await executeReadOnlyQuery(
      connection,
      "SELECT 1 AS value, 2 AS value",
      10,
      5,
    );
    assert.deepEqual(result.columns, [
      { name: "value", type: "INTEGER" },
      { name: "value_1", type: "INTEGER" },
    ]);
  } finally {
    connection.closeSync();
  }
});

test("query planning resolves only configured base tables through CTEs", async () => {
  const connection = await DuckDBConnection.create();
  try {
    const tables = await planReadOnlyQuery(
      connection,
      queryConfig,
      "WITH recent AS (SELECT * FROM alpha.events) " +
        "SELECT * FROM recent JOIN beta.events USING (id)",
    );
    assert.deepEqual(
      tables.map(({ database, table }) => `${database}.${table.name}`).sort(),
      ["alpha.events", "beta.events"],
    );
  } finally {
    connection.closeSync();
  }
});

test("query planning resolves unqualified tables and rejects external table functions", async () => {
  const connection = await DuckDBConnection.create();
  try {
    const tables = await planReadOnlyQuery(
      connection,
      queryConfig,
      "SELECT * FROM metrics",
      "alpha",
    );
    assert.equal(tables[0].database, "alpha");
    assert.equal(tables[0].table.name, "metrics");
    await assert.rejects(
      planReadOnlyQuery(connection, queryConfig, "FROM read_parquet('/tmp/data.parquet')"),
      { code: "SQL_REJECTED" },
    );
    await assert.rejects(
      planReadOnlyQuery(connection, queryConfig, "SELECT current_setting('threads')"),
      { code: "SQL_REJECTED" },
    );
    await assert.rejects(
      planReadOnlyQuery(connection, queryConfig, "SELECT write_log('probe')"),
      { code: "SQL_REJECTED" },
    );
  } finally {
    connection.closeSync();
  }
});

test("query planning narrows safe single-table dt ranges", async () => {
  const connection = await DuckDBConnection.create();
  try {
    const equality = await planReadOnlyQuery(
      connection,
      queryConfig,
      "SELECT count(*) FROM alpha.events e WHERE e.dt = '2026-07-22'",
    );
    assert.deepEqual(equality[0].partitionDates, ["2026-07-22"]);

    const range = await planReadOnlyQuery(
      connection,
      queryConfig,
      "SELECT count(*) FROM alpha.events " +
        "WHERE dt >= '2026-07-20' AND dt <= DATE '2026-07-22'",
    );
    assert.deepEqual(range[0].partitionDates, [
      "2026-07-20",
      "2026-07-21",
      "2026-07-22",
    ]);
  } finally {
    connection.closeSync();
  }
});

test("partition globs list only the selected date directories", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ebb-query-partitions-"));
  for (const date of ["2026-07-20", "2026-07-21", "2026-07-22"]) {
    const directory = path.join(root, `dt=${date}`);
    fs.mkdirSync(directory);
    fs.writeFileSync(path.join(directory, `events-${date.slice(-2)}.parquet`), "");
  }

  assert.equal(
    partitionGlobExpression("s3://archive/events/", ["2026-07-22"]),
    "'s3://archive/events/dt=2026-07-22/*.parquet'",
  );

  const connection = await DuckDBConnection.create();
  try {
    const expression = partitionGlobExpression(`${root}/`, [
      "2026-07-20",
      "2026-07-22",
    ]);
    assert.doesNotMatch(expression, /dt=\*/);
    const reader = await connection.runAndReadAll(
      `SELECT file FROM glob(${expression}) ORDER BY file`,
    );
    assert.deepEqual(
      reader.getRows().map(([file]) => path.basename(file)),
      ["events-20.parquet", "events-22.parquet"],
    );
  } finally {
    connection.closeSync();
  }
});

test("query planning does not narrow ambiguous predicates or repeated tables", async () => {
  const connection = await DuckDBConnection.create();
  try {
    const disjunction = await planReadOnlyQuery(
      connection,
      queryConfig,
      "SELECT * FROM alpha.events WHERE dt = '2026-07-22' OR id = 1",
    );
    assert.equal(disjunction[0].partitionDates, undefined);

    const repeated = await planReadOnlyQuery(
      connection,
      queryConfig,
      "SELECT * FROM alpha.events a JOIN alpha.events b ON a.id = b.id " +
        "WHERE a.dt = '2026-07-22'",
    );
    assert.equal(repeated[0].partitionDates, undefined);
  } finally {
    connection.closeSync();
  }
});

test("query timeout covers statement preparation", async () => {
  let rejectPreparation;
  const connection = {
    async extractStatements() {
      return {
        count: 1,
        prepare() {
          return new Promise((resolve, reject) => {
            rejectPreparation = reject;
          });
        },
      };
    },
    interrupt() {
      rejectPreparation(new Error("interrupted"));
    },
  };

  await assert.rejects(executeReadOnlyQuery(connection, "SELECT 1", 1, 0.01), {
    code: "QUERY_TIMEOUT",
  });
});

test("query results enforce a serialized byte limit", async () => {
  const connection = await DuckDBConnection.create();
  try {
    await assert.rejects(
      executeReadOnlyQuery(connection, "SELECT repeat('x', 2000)", 1, 5, 5, 1000),
      { code: "RESULT_TOO_LARGE" },
    );
  } finally {
    connection.closeSync();
  }
});
