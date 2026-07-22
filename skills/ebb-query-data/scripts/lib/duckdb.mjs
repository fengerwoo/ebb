import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { DuckDBInstance, StatementType } from "@duckdb/node-api";

import { redactSecrets } from "./config.mjs";
import { EbbQueryError, fail } from "./errors.mjs";

const CACHE_ROOT =
  process.env.XDG_CACHE_HOME ||
  (process.platform === "win32" && process.env.LOCALAPPDATA) ||
  path.join(os.homedir(), ".cache");
const EXTENSION_DIRECTORY = path.join(
  CACHE_ROOT,
  "ebb-query-data",
  "duckdb-extensions",
);
const MAX_PARTITION_DAYS = 400;
const MAX_EXPLICIT_FILES = 10_000;
const BLOCKED_SCALAR_FUNCTIONS = new Set([
  "current_setting",
  "currval",
  "getenv",
  "getvariable",
  "nextval",
  "setseed",
  "setvariable",
  "write_log",
]);

function quoteIdentifier(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function quoteLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

export function partitionGlobExpression(scope, dates) {
  const patterns = dates.map((date) => `${scope}dt=${date}/*.parquet`);
  return patterns.length === 1
    ? quoteLiteral(patterns[0])
    : `[${patterns.map(quoteLiteral).join(", ")}]`;
}

function tableKey(database, table) {
  return `${database}.${table}`;
}

function deduplicateNames(names) {
  const used = new Set();
  return names.map((name) => {
    let candidate = name;
    let suffix = 1;
    while (used.has(candidate)) {
      candidate = `${name}_${suffix++}`;
    }
    used.add(candidate);
    return candidate;
  });
}

function queryTimeout(timeoutSeconds) {
  fail("QUERY_TIMEOUT", `query exceeded ${timeoutSeconds} seconds`, 5);
}

async function withDeadline(connection, deadline, timeoutSeconds, operation) {
  const remaining = deadline - Date.now();
  if (remaining <= 0) {
    queryTimeout(timeoutSeconds);
  }

  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    connection.interrupt();
  }, remaining);
  timer.unref?.();

  try {
    return await operation();
  } catch (error) {
    if (timedOut || Date.now() >= deadline) {
      queryTimeout(timeoutSeconds);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function loadHttpfs(run) {
  try {
    await run("LOAD httpfs");
  } catch (error) {
    if (error instanceof EbbQueryError) {
      throw error;
    }
    await run("INSTALL httpfs");
    await run("LOAD httpfs");
  }
}

function storageSecretParts(storage, scope) {
  const parts = [
    "TYPE s3",
    `KEY_ID ${quoteLiteral(storage.accessKeyId)}`,
    `SECRET ${quoteLiteral(storage.secretAccessKey)}`,
    `URL_STYLE ${quoteLiteral(storage.urlStyle)}`,
    `USE_SSL ${storage.useSsl ? "true" : "false"}`,
    `SCOPE ${quoteLiteral(scope)}`,
  ];
  if (storage.sessionToken) {
    parts.push(`SESSION_TOKEN ${quoteLiteral(storage.sessionToken)}`);
  }
  if (storage.region) {
    parts.push(`REGION ${quoteLiteral(storage.region)}`);
  }
  if (storage.endpoint) {
    parts.push(`ENDPOINT ${quoteLiteral(storage.endpoint)}`);
  }
  return parts;
}

export async function validateSelectStatement(connection, sql) {
  let extracted;
  try {
    extracted = await connection.extractStatements(sql);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    fail("SQL_INVALID", message, 3);
  }
  if (extracted.count !== 1) {
    fail("SQL_REJECTED", "exactly one SQL statement is allowed", 3);
  }

  let prepared;
  try {
    prepared = await extracted.prepare(0);
    if (prepared.statementType !== StatementType.SELECT) {
      const actual = StatementType[prepared.statementType] || String(prepared.statementType);
      fail("SQL_REJECTED", `only SELECT, WITH, or FROM queries are allowed (got ${actual})`, 3);
    }
  } finally {
    prepared?.destroySync();
  }
}

async function parseSqlAst(connection, sql) {
  let reader;
  try {
    reader = await connection.runAndReadAll(
      `SELECT json_serialize_sql(${quoteLiteral(sql)})`,
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    fail("SQL_INVALID", message, 3);
  }

  let parsed;
  try {
    parsed = JSON.parse(reader.getRows()[0][0]);
  } catch {
    fail("SQL_INVALID", "DuckDB could not parse the SQL statement", 3);
  }
  if (parsed.error) {
    fail("SQL_INVALID", parsed.error_message || "invalid SQL statement", 3);
  }
  if (parsed.statements.length !== 1) {
    fail("SQL_REJECTED", "exactly one SQL statement is allowed", 3);
  }

  const node = parsed.statements[0]?.node;
  if (node?.type !== "SELECT_NODE") {
    const actual = node?.type?.replace(/_NODE$/, "") || "UNKNOWN";
    fail(
      "SQL_REJECTED",
      `only SELECT, WITH, or FROM queries are allowed (got ${actual})`,
      3,
    );
  }
  return node;
}

function configuredName(values, requested) {
  if (values[requested]) {
    return requested;
  }
  const folded = requested.toLowerCase();
  return Object.keys(values).find((name) => name.toLowerCase() === folded) || null;
}

function resolveQueryTable(config, defaultDatabase, schemaName, tableName) {
  if (!schemaName && !defaultDatabase) {
    fail(
      "DATABASE_REQUIRED",
      `no database selected for table ${tableName}; qualify it or pass --database`,
      2,
    );
  }

  const requestedDatabase = schemaName || defaultDatabase;
  const database = configuredName(config.databases, requestedDatabase);
  if (!database) {
    fail("DATABASE_NOT_FOUND", `database not found: ${requestedDatabase}`, 2);
  }
  const table = configuredName(config.databases[database].tables, tableName);
  if (!table) {
    fail("TABLE_NOT_FOUND", `table not found: ${database}.${tableName}`, 2);
  }
  return { database, table: config.databases[database].tables[table] };
}

function collectQueryTables(node, config, defaultDatabase) {
  const references = new Map();

  function visit(value, visibleCtes = new Set()) {
    if (Array.isArray(value)) {
      for (const item of value) {
        visit(item, visibleCtes);
      }
      return;
    }
    if (!value || typeof value !== "object") {
      return;
    }

    if (value.type === "SELECT_NODE") {
      const scopedCtes = new Set(visibleCtes);
      const ctes = value.cte_map?.map || [];
      for (const entry of ctes) {
        scopedCtes.add(String(entry.key).toLowerCase());
      }
      for (const entry of ctes) {
        visit(entry.value?.query, scopedCtes);
      }
      for (const [key, child] of Object.entries(value)) {
        if (key !== "cte_map") {
          visit(child, scopedCtes);
        }
      }
      return;
    }

    if (value.type === "BASE_TABLE") {
      if (value.catalog_name) {
        fail("SQL_REJECTED", "three-part catalog references are not allowed", 3);
      }
      if (
        !value.schema_name &&
        visibleCtes.has(String(value.table_name).toLowerCase())
      ) {
        return;
      }
      const resolved = resolveQueryTable(
        config,
        defaultDatabase,
        value.schema_name,
        value.table_name,
      );
      const key = tableKey(resolved.database, resolved.table.name);
      references.set(key, {
        ...resolved,
        occurrences: (references.get(key)?.occurrences || 0) + 1,
      });
      return;
    }

    if (value.type === "TABLE_FUNCTION") {
      const functionName = value.function?.function_name || "unknown";
      fail("SQL_REJECTED", `table functions are not allowed (got ${functionName})`, 3);
    }

    if (
      value.class === "FUNCTION" &&
      BLOCKED_SCALAR_FUNCTIONS.has(String(value.function_name).toLowerCase())
    ) {
      fail(
        "SQL_REJECTED",
        `function ${value.function_name} is not allowed in read-only queries`,
        3,
      );
    }

    for (const child of Object.values(value)) {
      visit(child, visibleCtes);
    }
  }

  visit(node);
  return [...references.values()];
}

function parseDateExpression(expression) {
  let value = expression;
  if (value?.class === "CAST" && value.cast_type?.id === "DATE") {
    value = value.child;
  }
  if (
    value?.class !== "CONSTANT" ||
    value.value?.is_null ||
    typeof value.value?.value !== "string"
  ) {
    return null;
  }
  const date = value.value.value;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return null;
  }
  const parsed = new Date(`${date}T00:00:00Z`);
  return Number.isNaN(parsed.valueOf()) || parsed.toISOString().slice(0, 10) !== date
    ? null
    : date;
}

function isPartitionColumn(expression, baseTable) {
  if (expression?.class !== "COLUMN_REF") {
    return false;
  }
  const names = expression.column_names || [];
  if (String(names.at(-1)).toLowerCase() !== "dt") {
    return false;
  }
  if (names.length === 1) {
    return true;
  }

  const qualifier = names.slice(0, -1).map((name) => String(name).toLowerCase());
  const alias = String(baseTable.alias || "").toLowerCase();
  const table = String(baseTable.table_name || "").toLowerCase();
  const schema = String(baseTable.schema_name || "").toLowerCase();
  return (
    (qualifier.length === 1 && qualifier[0] === (alias || table)) ||
    (qualifier.length === 2 && qualifier[0] === schema && qualifier[1] === table)
  );
}

function directDateBounds(expression, baseTable) {
  if (!expression || typeof expression !== "object") {
    return null;
  }
  if (expression.class === "BETWEEN" && isPartitionColumn(expression.input, baseTable)) {
    const lower = parseDateExpression(expression.lower);
    const upper = parseDateExpression(expression.upper);
    return lower && upper ? { lower, upper } : null;
  }
  if (expression.class !== "COMPARISON") {
    return null;
  }

  let column = expression.left;
  let constant = expression.right;
  let comparison = expression.type;
  if (!isPartitionColumn(column, baseTable) && isPartitionColumn(constant, baseTable)) {
    [column, constant] = [constant, column];
    comparison = new Map([
      ["COMPARE_GREATERTHAN", "COMPARE_LESSTHAN"],
      ["COMPARE_GREATERTHANOREQUALTO", "COMPARE_LESSTHANOREQUALTO"],
      ["COMPARE_LESSTHAN", "COMPARE_GREATERTHAN"],
      ["COMPARE_LESSTHANOREQUALTO", "COMPARE_GREATERTHANOREQUALTO"],
    ]).get(comparison) || comparison;
  }
  if (!isPartitionColumn(column, baseTable)) {
    return null;
  }

  const date = parseDateExpression(constant);
  if (!date) {
    return null;
  }
  if (comparison === "COMPARE_EQUAL") {
    return { lower: date, upper: date };
  }
  if (comparison === "COMPARE_GREATERTHANOREQUALTO") {
    return { lower: date };
  }
  if (comparison === "COMPARE_LESSTHANOREQUALTO") {
    return { upper: date };
  }
  return null;
}

function partitionDates(node, references) {
  const baseTable = node.from_table;
  if (
    references.length !== 1 ||
    references[0].occurrences !== 1 ||
    baseTable?.type !== "BASE_TABLE" ||
    String(baseTable.table_name).toLowerCase() !==
      references[0].table.name.toLowerCase() ||
    (baseTable.schema_name &&
      String(baseTable.schema_name).toLowerCase() !== references[0].database.toLowerCase())
  ) {
    return null;
  }

  const where = node.where_clause;
  const terms = where?.type === "CONJUNCTION_AND" ? where.children : [where];
  let lower = null;
  let upper = null;
  for (const term of terms) {
    const bounds = directDateBounds(term, baseTable);
    if (bounds?.lower && (!lower || bounds.lower > lower)) {
      lower = bounds.lower;
    }
    if (bounds?.upper && (!upper || bounds.upper < upper)) {
      upper = bounds.upper;
    }
  }
  if (!lower || !upper || lower > upper) {
    return null;
  }

  const dates = [];
  const end = new Date(`${upper}T00:00:00Z`).valueOf();
  for (
    let current = new Date(`${lower}T00:00:00Z`).valueOf();
    current <= end && dates.length <= MAX_PARTITION_DAYS;
    current += 86_400_000
  ) {
    dates.push(new Date(current).toISOString().slice(0, 10));
  }
  return dates.length <= MAX_PARTITION_DAYS ? dates : null;
}

export async function planReadOnlyQuery(connection, config, sql, defaultDatabase = null) {
  const node = await parseSqlAst(connection, sql);
  const references = collectQueryTables(node, config, defaultDatabase);
  const dates = partitionDates(node, references);
  if (dates) {
    references[0].partitionDates = dates;
  }
  return references;
}

function enforceResultBytes(value, limit) {
  let total = 0;

  function add(bytes) {
    total += bytes;
    if (total > limit) {
      fail(
        "RESULT_TOO_LARGE",
        `result exceeded configured max_result_bytes (${limit})`,
        5,
      );
    }
  }

  function visit(item) {
    if (item === null || item === undefined) {
      add(4);
    } else if (typeof item === "string") {
      add(Buffer.byteLength(item, "utf8") + 2);
    } else if (typeof item === "number" || typeof item === "boolean") {
      add(String(item).length);
    } else if (Array.isArray(item)) {
      add(2 + Math.max(item.length - 1, 0));
      for (const child of item) visit(child);
    } else if (typeof item === "object") {
      const entries = Object.entries(item);
      add(2 + Math.max(entries.length - 1, 0));
      for (const [key, child] of entries) {
        add(Buffer.byteLength(key, "utf8") + 3);
        visit(child);
      }
    } else {
      add(Buffer.byteLength(String(item), "utf8") + 2);
    }
  }

  visit(value);
}

function resultFromReader(reader, maxRows, maxResultBytes = 50_000_000) {
  const names = deduplicateNames(reader.columnNames());
  const types = reader.columnTypes().map((type) => type.toString());
  let rows = reader.getRowsJson();
  const truncated = rows.length > maxRows;
  if (truncated) {
    rows = rows.slice(0, maxRows);
  }
  enforceResultBytes(rows, maxResultBytes);
  return {
    columns: names.map((name, index) => ({ name, type: types[index] })),
    rows,
    rowCount: rows.length,
    truncated,
  };
}

export async function executeReadOnlyQuery(
  connection,
  sql,
  maxRows,
  timeoutSeconds,
  reportedTimeoutSeconds = timeoutSeconds,
  maxResultBytes = 50_000_000,
) {
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    connection.interrupt();
  }, timeoutSeconds * 1_000);
  timer.unref?.();

  try {
    await validateSelectStatement(connection, sql);
    const reader = await connection.streamAndReadUntil(sql, maxRows + 1);
    return resultFromReader(reader, maxRows, maxResultBytes);
  } catch (error) {
    if (timedOut) {
      fail("QUERY_TIMEOUT", `query exceeded ${reportedTimeoutSeconds} seconds`, 5);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export async function openQueryEngine(
  config,
  {
    database = null,
    sql = null,
    tables = [],
    timeoutSeconds = config.query.timeoutSeconds,
  } = {},
) {
  fs.mkdirSync(EXTENSION_DIRECTORY, { recursive: true });
  const instance = await DuckDBInstance.create(":memory:", {
    extension_directory: EXTENSION_DIRECTORY,
    memory_limit: "512MB",
    threads: 4,
  });
  const connection = await instance.connect();
  const registrationErrors = new Map();
  const deadline = Date.now() + timeoutSeconds * 1_000;
  const run = (statement) =>
    withDeadline(connection, deadline, timeoutSeconds, () => connection.run(statement));

  try {
    const plannedTables = sql
      ? await withDeadline(connection, deadline, timeoutSeconds, () =>
          planReadOnlyQuery(connection, config, sql, database),
        )
      : [];
    const selectedTables = new Map();
    for (const reference of [...tables, ...plannedTables]) {
      selectedTables.set(tableKey(reference.database, reference.table.name), reference);
    }

    for (const databaseName of Object.keys(config.databases)) {
      await run(`CREATE SCHEMA ${quoteIdentifier(databaseName)}`);
    }

    if (selectedTables.size > 0) {
      await loadHttpfs(run);
      await run("SET allow_unredacted_secrets = false");
      await run(`SET http_timeout = ${Math.min(timeoutSeconds, 30)}`);
      await run("SET http_retries = 1");
    }
    await run("SET autoinstall_known_extensions = false");
    await run("SET autoload_known_extensions = false");
    await run("SET allow_community_extensions = false");

    let secretIndex = 0;
    const scopes = new Map();
    for (const {
      database: databaseName,
      table,
      sampleLatest = false,
      partitionDates: selectedDates = null,
    } of selectedTables.values()) {
      const storage = config.storages[table.storage];
      const scope = `s3://${storage.bucket}/${table.prefix}/`;
      let secretName = scopes.get(`${table.storage}\u0000${table.prefix}`);
      if (!secretName) {
        secretName = `ebb_query_${secretIndex++}`;
        scopes.set(`${table.storage}\u0000${table.prefix}`, secretName);
        const parts = storageSecretParts(storage, scope);
        await run(
          `CREATE OR REPLACE SECRET ${quoteIdentifier(secretName)} (${parts.join(", ")})`,
        );
      }

      const glob = `${scope}dt=*/*.parquet`;
      try {
        let sourceSql = quoteLiteral(glob);
        let emptySelection = false;
        let narrowedToFiles = false;
        if (selectedDates) {
          const selectedGlob = partitionGlobExpression(scope, selectedDates);
          const reader = await withDeadline(
            connection,
            deadline,
            timeoutSeconds,
            () =>
              connection.runAndReadAll(
                `SELECT file FROM glob(${selectedGlob}) ORDER BY ` +
                  "try_cast(regexp_extract(file, '-([0-9]+)[.]parquet$', 1) " +
                  `AS UBIGINT) DESC NULLS LAST, file DESC LIMIT ${MAX_EXPLICIT_FILES + 1}`,
              ),
          );
          const files = reader.getRows().map((row) => row[0]);
          if (files.length <= MAX_EXPLICIT_FILES) {
            narrowedToFiles = true;
            if (files.length > 0) {
              sourceSql =
                files.length === 1
                  ? quoteLiteral(files[0])
                  : `[${files.map(quoteLiteral).join(", ")}]`;
            } else {
              emptySelection = true;
            }
          }
        }
        if (sampleLatest || emptySelection) {
          const reader = await withDeadline(
            connection,
            deadline,
            timeoutSeconds,
            () =>
              connection.runAndReadAll(
                `SELECT file FROM glob(${quoteLiteral(glob)}) ` +
                  "ORDER BY try_cast(regexp_extract(file, '-([0-9]+)[.]parquet$', 1) " +
                  "AS UBIGINT) DESC NULLS LAST, file DESC LIMIT 1",
              ),
          );
          const rows = reader.getRows();
          if (rows.length === 0) {
            throw new Error("no Parquet files found under the configured prefix");
          }
          sourceSql = quoteLiteral(rows[0][0]);
        }
        const singleDayFastPath =
          narrowedToFiles && selectedDates.length === 1 && !emptySelection;
        const unionByName = !singleDayFastPath;
        await run(
          `CREATE VIEW ${quoteIdentifier(databaseName)}.${quoteIdentifier(table.name)} AS ` +
            `SELECT * FROM read_parquet(${sourceSql}, ` +
            `hive_partitioning = true, union_by_name = ${unionByName})` +
            (emptySelection ? " WHERE false" : ""),
        );
      } catch (error) {
        if (error instanceof EbbQueryError) {
          throw error;
        }
        const message = error instanceof Error ? error.message : String(error);
        registrationErrors.set(
          tableKey(databaseName, table.name),
          redactSecrets(message, config),
        );
      }
    }

    if (database) {
      await run(`SET search_path = ${quoteLiteral(database)}`);
    }

    // Register credentials and views first, then close every filesystem except scoped S3.
    await run("SET disabled_filesystems = 'LocalFileSystem,HTTPFileSystem'");
    await run("SET lock_configuration = true");
  } catch (error) {
    connection.closeSync();
    instance.closeSync();
    if (error instanceof EbbQueryError) {
      throw error;
    }
    const message = error instanceof Error ? error.message : String(error);
    fail("ENGINE_INIT_FAILED", redactSecrets(message, config), 4);
  }

  return {
    async query(sql, { maxRows, maxResultBytes }) {
      try {
        const remainingSeconds = Math.max((deadline - Date.now()) / 1_000, 0.001);
        return await executeReadOnlyQuery(
          connection,
          sql,
          maxRows,
          remainingSeconds,
          timeoutSeconds,
          maxResultBytes,
        );
      } catch (error) {
        if (error instanceof EbbQueryError) {
          throw error;
        }
        const message = error instanceof Error ? error.message : String(error);
        const unavailable = [...registrationErrors.keys()];
        const suffix = unavailable.length
          ? `; unavailable archive tables: ${unavailable.join(", ")}`
          : "";
        fail("QUERY_FAILED", `${redactSecrets(message, config)}${suffix}`, 4);
      }
    },

    async describe(databaseName, tableName) {
      const key = tableKey(databaseName, tableName);
      if (registrationErrors.has(key)) {
        fail(
          "TABLE_UNAVAILABLE",
          `cannot open ${key}: ${registrationErrors.get(key)}`,
          4,
        );
      }
      const sql =
        `DESCRIBE SELECT * FROM ${quoteIdentifier(databaseName)}.${quoteIdentifier(tableName)}`;
      try {
        const reader = await connection.runAndReadAll(sql);
        return resultFromReader(
          reader,
          Number.MAX_SAFE_INTEGER,
          config.query.maxResultBytes,
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        fail("QUERY_FAILED", redactSecrets(message, config), 4);
      }
    },

    close() {
      connection.closeSync();
      instance.closeSync();
    },
  };
}
