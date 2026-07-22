import { fail } from "./errors.mjs";

const IDENTIFIER = "[A-Za-z_][A-Za-z0-9_]*";

function result(columns, rows) {
  return {
    columns: columns.map(([name, type]) => ({ name, type })),
    rows,
    rowCount: rows.length,
    truncated: false,
  };
}

function selectedDatabase(config, requested) {
  const name = requested || config.query.defaultDatabase;
  if (!name) {
    fail(
      "DATABASE_REQUIRED",
      "no database selected; use SHOW TABLES FROM <database> or pass --database",
      2,
    );
  }
  if (!config.databases[name]) {
    fail("DATABASE_NOT_FOUND", `database not found: ${name}`, 2);
  }
  return name;
}

function stripMetaTerminator(sql) {
  return sql.trim().replace(/;\s*$/, "").trim();
}

export function parseMetaCommand(sql, defaultDatabase = null) {
  const normalized = stripMetaTerminator(sql);
  let match;

  if (/^SHOW\s+DATABASES$/i.test(normalized)) {
    return { kind: "show-databases" };
  }

  match = normalized.match(
    new RegExp(`^SHOW\\s+TABLES(?:\\s+(?:FROM|IN)\\s+(${IDENTIFIER}))?$`, "i"),
  );
  if (match) {
    return { kind: "show-tables", database: match[1] || defaultDatabase };
  }

  match = normalized.match(
    new RegExp(`^(?:DESCRIBE|DESC)\\s+(?:(${IDENTIFIER})\\.)?(${IDENTIFIER})$`, "i"),
  );
  if (match) {
    return {
      kind: "describe-table",
      database: match[1] || defaultDatabase,
      table: match[2],
    };
  }

  match = normalized.match(
    new RegExp(
      `^SHOW\\s+(?:COLUMNS|FIELDS)\\s+FROM\\s+(?:(${IDENTIFIER})\\.)?(${IDENTIFIER})$`,
      "i",
    ),
  );
  if (match) {
    return {
      kind: "describe-table",
      database: match[1] || defaultDatabase,
      table: match[2],
    };
  }

  if (/^(?:SHOW|DESCRIBE|DESC)\b/i.test(normalized)) {
    fail("SQL_UNSUPPORTED", "unsupported metadata statement", 3);
  }
  return null;
}

export function validateDatabase(config, database) {
  if (!database) {
    return null;
  }
  if (!config.databases[database]) {
    fail("DATABASE_NOT_FOUND", `database not found: ${database}`, 2);
  }
  return database;
}

export function runCatalogCommand(config, command) {
  if (command.kind === "show-databases") {
    const rows = Object.keys(config.databases)
      .sort()
      .map((name) => [name]);
    return result([["database", "VARCHAR"]], rows);
  }

  if (command.kind === "show-tables") {
    const databaseName = selectedDatabase(config, command.database);
    const rows = Object.keys(config.databases[databaseName].tables)
      .sort()
      .map((name) => [name]);
    return result([["table", "VARCHAR"]], rows);
  }

  fail("INTERNAL_ERROR", `unsupported catalog command: ${command.kind}`);
}

export function resolveTable(config, databaseName, tableName) {
  const database = selectedDatabase(config, databaseName);
  const table = config.databases[database].tables[tableName];
  if (!table) {
    fail("TABLE_NOT_FOUND", `table not found: ${database}.${tableName}`, 2);
  }
  return { database, table };
}
