#!/usr/bin/env node

import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { parseMetaCommand, resolveTable, runCatalogCommand, validateDatabase } from "./lib/catalog.mjs";
import { expandHome, loadConfig, resolveConfigPath } from "./lib/config.mjs";
import { openQueryEngine } from "./lib/duckdb.mjs";
import { EbbQueryError, errorPayload, fail } from "./lib/errors.mjs";
import { runProcessWithTimeout } from "./lib/isolation.mjs";
import { renderResult, writeResult } from "./lib/output.mjs";

const VERSION = "0.1.1";
const WORKER_PATH = fileURLToPath(new URL("./ebb-query-data-worker.mjs", import.meta.url));

const HELP = `ebb-query-data ${VERSION}

Usage:
  ebb-query-data --project-root <path> --sql <sql> [options]
  ebb-query-data --config <path> --sql <sql> [options]

Options:
  --project-root <path>     Project containing ebb-query-data.yaml
  --config <path>           Explicit config path; disables config fallback
  --sql <sql>               One metadata command or read-only SQL query
  --database <name>         Default database for unqualified table names
  --format <json|csv>       Output format (default: json)
  --output <path>           Create a project-relative file; never overwrites
  --max-rows <number>       Result row limit, capped by config
  --timeout-seconds <n>     Query timeout, capped by config
  --no-header               Omit the CSV header
  --help                    Show this help
  --version                 Show the version

Metadata commands:
  SHOW DATABASES;
  SHOW TABLES FROM <database>;
  DESCRIBE <database>.<table>;
  SHOW COLUMNS FROM <database>.<table>;
`;

const VALUE_OPTIONS = new Set([
  "--project-root",
  "--config",
  "--sql",
  "--database",
  "--format",
  "--output",
  "--max-rows",
  "--timeout-seconds",
]);

function parsePositiveInteger(value, option) {
  if (!/^\d+$/.test(value)) {
    fail("ARGUMENT_INVALID", `${option} must be a positive integer`, 2);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    fail("ARGUMENT_INVALID", `${option} must be a positive integer`, 2);
  }
  return parsed;
}

export function parseArgs(argv) {
  const args = {
    projectRoot: null,
    configPath: null,
    sql: null,
    database: null,
    format: "json",
    outputPath: null,
    maxRows: null,
    timeoutSeconds: null,
    header: true,
    help: false,
    version: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    let token = argv[index];
    let inlineValue = null;
    const equals = token.indexOf("=");
    if (equals > 0) {
      inlineValue = token.slice(equals + 1);
      token = token.slice(0, equals);
    }

    if (token === "--help" || token === "-h") {
      args.help = true;
      continue;
    }
    if (token === "--version") {
      args.version = true;
      continue;
    }
    if (token === "--no-header") {
      args.header = false;
      continue;
    }
    if (!VALUE_OPTIONS.has(token)) {
      fail("ARGUMENT_INVALID", `unknown argument: ${token}`, 2);
    }

    const value = inlineValue ?? argv[++index];
    if (value === undefined || value === "") {
      fail("ARGUMENT_INVALID", `${token} requires a value`, 2);
    }
    if (token === "--project-root") args.projectRoot = value;
    if (token === "--config") args.configPath = value;
    if (token === "--sql") args.sql = value;
    if (token === "--database") args.database = value;
    if (token === "--format") args.format = value.toLowerCase();
    if (token === "--output") args.outputPath = value;
    if (token === "--max-rows") args.maxRows = parsePositiveInteger(value, token);
    if (token === "--timeout-seconds") {
      args.timeoutSeconds = parsePositiveInteger(value, token);
    }
  }

  if (!new Set(["json", "csv"]).has(args.format)) {
    fail("ARGUMENT_INVALID", "--format must be json or csv", 2);
  }
  if (!args.help && !args.version && (!args.sql || args.sql.trim() === "")) {
    fail("ARGUMENT_INVALID", "--sql is required", 2);
  }
  if (!args.header && args.format !== "csv") {
    fail("ARGUMENT_INVALID", "--no-header is only valid with --format csv", 2);
  }
  return args;
}

function elapsedMilliseconds(started) {
  return Math.round(Number(process.hrtime.bigint() - started) / 1_000_000);
}

function warnIfTruncated(result, stderr = process.stderr) {
  if (result.truncated) {
    stderr.write(
      `${JSON.stringify({ warning: { code: "RESULT_TRUNCATED", rowCount: result.rowCount } })}\n`,
    );
  }
}

export async function run(argv, runtime = {}) {
  const stdout = runtime.stdout ?? process.stdout;
  const stderr = runtime.stderr ?? process.stderr;
  const args = parseArgs(argv);
  if (args.help) {
    stdout.write(HELP);
    return 0;
  }
  if (args.version) {
    stdout.write(`${VERSION}\n`);
    return 0;
  }

  const cwd = runtime.cwd ?? process.cwd();
  const homeDir = runtime.homeDir;
  const configPath = resolveConfigPath({
    configPath: args.configPath,
    projectRoot: args.projectRoot,
    cwd,
    ...(homeDir ? { homeDir } : {}),
  });
  const config = loadConfig(configPath);
  const database = validateDatabase(config, args.database || config.query.defaultDatabase);
  const projectRoot = args.projectRoot
    ? path.resolve(cwd, expandHome(args.projectRoot, homeDir))
    : cwd;
  const maxRows = Math.min(args.maxRows ?? config.query.maxRows, config.query.maxRows);
  const timeoutSeconds = Math.min(
    args.timeoutSeconds ?? config.query.timeoutSeconds,
    config.query.timeoutSeconds,
  );
  const started = process.hrtime.bigint();
  const meta = parseMetaCommand(args.sql, database);

  let result;
  if (meta && meta.kind !== "describe-table") {
    result = runCatalogCommand(config, meta);
  } else {
    let tables = [];
    if (meta?.kind === "describe-table") {
      tables = [
        {
          ...resolveTable(config, meta.database, meta.table),
          sampleLatest: true,
        },
      ];
    }
    const engine = await openQueryEngine(config, {
      database,
      sql: meta ? null : args.sql,
      tables,
      timeoutSeconds,
    });
    try {
      if (meta?.kind === "describe-table") {
        result = await engine.describe(tables[0].database, tables[0].table.name);
      } else {
        result = await engine.query(args.sql, {
          maxRows,
          maxResultBytes: config.query.maxResultBytes,
        });
      }
    } finally {
      engine.close();
    }
  }

  result.elapsedMs = elapsedMilliseconds(started);
  const content = renderResult(result, args.format, { header: args.header });
  writeResult({
    content,
    outputPath: args.outputPath,
    projectRoot,
    format: args.format,
    result,
    stdout,
  });
  warnIfTruncated(result, stderr);
  return 0;
}

function engineTimeout(argv) {
  const args = parseArgs(argv);
  if (args.help || args.version) {
    return null;
  }
  const meta = parseMetaCommand(args.sql, args.database);
  if (meta && meta.kind !== "describe-table") {
    return null;
  }

  const configPath = resolveConfigPath({
    configPath: args.configPath,
    projectRoot: args.projectRoot,
  });
  const config = loadConfig(configPath);
  return Math.min(
    args.timeoutSeconds ?? config.query.timeoutSeconds,
    config.query.timeoutSeconds,
  );
}

async function runIsolated(argv, timeoutSeconds) {
  const result = await runProcessWithTimeout(
    process.execPath,
    ["--max-old-space-size=256", WORKER_PATH, ...argv],
    {
      timeoutMs: timeoutSeconds * 1_000,
    },
  );
  if (result.timedOut) {
    const error = new EbbQueryError(
      "QUERY_TIMEOUT",
      `query exceeded ${timeoutSeconds} seconds`,
      { exitCode: 5 },
    );
    process.stderr.write(`${JSON.stringify(errorPayload(error))}\n`);
    return error.exitCode;
  }
  if (result.exitCode === null) {
    const error = new EbbQueryError(
      "WORKER_FAILED",
      `query worker exited due to signal ${result.signal || "unknown"}`,
      { exitCode: 4 },
    );
    process.stderr.write(`${JSON.stringify(errorPayload(error))}\n`);
    return error.exitCode;
  }
  return result.exitCode;
}

async function runWithErrors(argv) {
  try {
    return await run(argv);
  } catch (error) {
    const normalized =
      error instanceof EbbQueryError
        ? error
        : new EbbQueryError("INTERNAL_ERROR", error instanceof Error ? error.message : String(error));
    process.stderr.write(`${JSON.stringify(errorPayload(normalized))}\n`);
    return normalized.exitCode;
  }
}

export async function workerMain(argv = process.argv.slice(2)) {
  return runWithErrors(argv);
}

export async function main(argv = process.argv.slice(2)) {
  try {
    const timeoutSeconds = engineTimeout(argv);
    if (timeoutSeconds !== null) {
      return await runIsolated(argv, timeoutSeconds);
    }
  } catch (error) {
    const normalized =
      error instanceof EbbQueryError
        ? error
        : new EbbQueryError(
            "INTERNAL_ERROR",
            error instanceof Error ? error.message : String(error),
          );
    process.stderr.write(`${JSON.stringify(errorPayload(normalized))}\n`);
    return normalized.exitCode;
  }
  return runWithErrors(argv);
}

const invokedPath = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invokedPath === import.meta.url) {
  process.exitCode = await main();
}
