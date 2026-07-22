import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import YAML from "yaml";

import { fail } from "./errors.mjs";

export const CONFIG_FILENAME = "ebb-query-data.yaml";

const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const STORAGE_NAME_RE = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;
const RESERVED_DATABASES = new Set(["information_schema", "main", "pg_catalog", "temp"]);

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function configError(location, message) {
  fail("CONFIG_INVALID", `${location}: ${message}`, 2);
}

function requireObject(value, location) {
  if (!isObject(value)) {
    configError(location, "must be a mapping");
  }
  return value;
}

function requireString(value, location, { allowEmpty = false } = {}) {
  if (typeof value !== "string" || (!allowEmpty && value.trim() === "")) {
    configError(location, "must be a non-empty string");
  }
  return value.trim();
}

function optionalString(value, location, fallback = "") {
  if (value === undefined || value === null) {
    return fallback;
  }
  if (typeof value !== "string") {
    configError(location, "must be a string");
  }
  return value.trim();
}

function optionalBoolean(value, location, fallback) {
  if (value === undefined) {
    return fallback;
  }
  if (typeof value !== "boolean") {
    configError(location, "must be true or false");
  }
  return value;
}

function boundedInteger(value, location, fallback, min, max) {
  const resolved = value === undefined ? fallback : value;
  if (!Number.isSafeInteger(resolved) || resolved < min || resolved > max) {
    configError(location, `must be an integer between ${min} and ${max}`);
  }
  return resolved;
}

function assertKnownKeys(value, allowed, location) {
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) {
      configError(`${location}.${key}`, "unknown field");
    }
  }
}

function assertCaseInsensitiveNames(value, location) {
  const seen = new Map();
  for (const name of Object.keys(value)) {
    const folded = name.toLowerCase();
    if (seen.has(folded)) {
      configError(
        `${location}.${name}`,
        `conflicts case-insensitively with ${seen.get(folded)}`,
      );
    }
    seen.set(folded, name);
  }
}

function validateIdentifier(value, location, { database = false } = {}) {
  const name = requireString(value, location);
  if (!IDENTIFIER_RE.test(name)) {
    configError(location, "must match [A-Za-z_][A-Za-z0-9_]*");
  }
  if (database && RESERVED_DATABASES.has(name.toLowerCase())) {
    configError(location, `reserved database name: ${name}`);
  }
  return name;
}

function normalizeEndpoint(rawEndpoint, rawUseSsl, location) {
  let endpoint = optionalString(rawEndpoint, `${location}.endpoint`);
  let protocol;

  if (/^https?:\/\//i.test(endpoint)) {
    let parsed;
    try {
      parsed = new URL(endpoint);
    } catch {
      configError(`${location}.endpoint`, "must be a valid HTTP(S) endpoint");
    }
    if (parsed.username || parsed.password || parsed.search || parsed.hash) {
      configError(`${location}.endpoint`, "must not contain credentials, query, or fragment");
    }
    if (parsed.pathname && parsed.pathname !== "/") {
      configError(`${location}.endpoint`, "must not contain a path");
    }
    protocol = parsed.protocol;
    endpoint = parsed.host;
  } else {
    endpoint = endpoint.replace(/\/+$/, "");
  }

  const useSsl = optionalBoolean(rawUseSsl, `${location}.use_ssl`, protocol !== "http:");
  if (protocol && useSsl !== (protocol === "https:")) {
    configError(`${location}.use_ssl`, "conflicts with the endpoint URL scheme");
  }
  return { endpoint, useSsl };
}

function normalizeStorage(name, raw) {
  const location = `storages.${name}`;
  const value = requireObject(raw, location);
  assertKnownKeys(
    value,
    new Set([
      "type",
      "endpoint",
      "bucket",
      "access_key_id",
      "secret_access_key",
      "session_token",
      "region",
      "url_style",
      "use_ssl",
    ]),
    location,
  );

  const type = optionalString(value.type, `${location}.type`, "s3");
  if (type !== "s3") {
    configError(`${location}.type`, "only s3 is supported");
  }

  const bucket = requireString(value.bucket, `${location}.bucket`);
  if (/[\s/]/.test(bucket)) {
    configError(`${location}.bucket`, "must not contain whitespace or slashes");
  }

  const urlStyle = optionalString(value.url_style, `${location}.url_style`, "vhost");
  if (!new Set(["vhost", "path"]).has(urlStyle)) {
    configError(`${location}.url_style`, "must be vhost or path");
  }

  const { endpoint, useSsl } = normalizeEndpoint(value.endpoint, value.use_ssl, location);
  return {
    name,
    type,
    endpoint,
    bucket,
    accessKeyId: requireString(value.access_key_id, `${location}.access_key_id`),
    secretAccessKey: requireString(
      value.secret_access_key,
      `${location}.secret_access_key`,
    ),
    sessionToken: optionalString(value.session_token, `${location}.session_token`),
    region: optionalString(value.region, `${location}.region`),
    urlStyle,
    useSsl,
  };
}

function normalizePrefix(value, location) {
  const prefix = requireString(value, location).replace(/^\/+|\/+$/g, "");
  const segments = prefix.split("/");
  if (!prefix || segments.some((segment) => segment === "" || segment === "." || segment === "..")) {
    configError(location, "must be a non-empty object prefix without . or .. segments");
  }
  if (/[*?\[\]{}]/.test(prefix)) {
    configError(location, "must not contain wildcard characters");
  }
  return prefix;
}

function normalizeTable(databaseName, tableName, raw, storages) {
  const location = `databases.${databaseName}.tables.${tableName}`;
  const value = requireObject(raw, location);
  assertKnownKeys(value, new Set(["storage", "prefix", "description"]), location);
  const storage = requireString(value.storage, `${location}.storage`);
  if (!storages[storage]) {
    configError(`${location}.storage`, `references unknown storage: ${storage}`);
  }
  return {
    name: validateIdentifier(tableName, `${location} (table name)`),
    storage,
    prefix: normalizePrefix(value.prefix, `${location}.prefix`),
    description: optionalString(value.description, `${location}.description`),
  };
}

function normalizeDatabase(name, raw, storages) {
  const location = `databases.${name}`;
  const value = requireObject(raw, location);
  assertKnownKeys(value, new Set(["description", "tables"]), location);
  const databaseName = validateIdentifier(name, `${location} (database name)`, {
    database: true,
  });
  const rawTables = requireObject(value.tables, `${location}.tables`);
  if (Object.keys(rawTables).length === 0) {
    configError(`${location}.tables`, "must contain at least one table");
  }
  assertCaseInsensitiveNames(rawTables, `${location}.tables`);
  const tables = Object.create(null);
  for (const [tableName, table] of Object.entries(rawTables)) {
    tables[tableName] = normalizeTable(databaseName, tableName, table, storages);
  }
  return {
    name: databaseName,
    description: optionalString(value.description, `${location}.description`),
    tables,
  };
}

export function validateConfig(raw) {
  const value = requireObject(raw, "config");
  assertKnownKeys(value, new Set(["version", "storages", "databases", "query"]), "config");
  if (value.version !== 1) {
    configError("version", "must be 1");
  }

  const rawStorages = requireObject(value.storages, "storages");
  if (Object.keys(rawStorages).length === 0) {
    configError("storages", "must contain at least one storage");
  }
  const storages = Object.create(null);
  for (const [name, storage] of Object.entries(rawStorages)) {
    if (!STORAGE_NAME_RE.test(name)) {
      configError(`storages.${name}`, "storage name contains unsupported characters");
    }
    storages[name] = normalizeStorage(name, storage);
  }

  const rawDatabases = requireObject(value.databases, "databases");
  if (Object.keys(rawDatabases).length === 0) {
    configError("databases", "must contain at least one database");
  }
  assertCaseInsensitiveNames(rawDatabases, "databases");
  const databases = Object.create(null);
  for (const [name, database] of Object.entries(rawDatabases)) {
    databases[name] = normalizeDatabase(name, database, storages);
  }

  const rawQuery = value.query === undefined ? {} : requireObject(value.query, "query");
  assertKnownKeys(
    rawQuery,
    new Set(["default_database", "max_rows", "max_result_bytes", "timeout_seconds"]),
    "query",
  );
  const defaultDatabase = optionalString(rawQuery.default_database, "query.default_database");
  if (defaultDatabase && !databases[defaultDatabase]) {
    configError("query.default_database", `references unknown database: ${defaultDatabase}`);
  }

  return {
    version: 1,
    storages,
    databases,
    query: {
      defaultDatabase: defaultDatabase || null,
      maxRows: boundedInteger(rawQuery.max_rows, "query.max_rows", 10_000, 1, 1_000_000),
      maxResultBytes: boundedInteger(
        rawQuery.max_result_bytes,
        "query.max_result_bytes",
        50_000_000,
        1_024,
        100_000_000,
      ),
      timeoutSeconds: boundedInteger(
        rawQuery.timeout_seconds,
        "query.timeout_seconds",
        900,
        1,
        3_600,
      ),
    },
  };
}

export function expandHome(input, homeDir = os.homedir()) {
  if (input === "~") {
    return homeDir;
  }
  if (input.startsWith("~/") || input.startsWith("~\\")) {
    return path.join(homeDir, input.slice(2));
  }
  return input;
}

function existingFile(candidate) {
  try {
    return fs.statSync(candidate).isFile();
  } catch {
    return false;
  }
}

export function resolveConfigPath({
  configPath,
  projectRoot,
  cwd = process.cwd(),
  homeDir = os.homedir(),
}) {
  if (configPath) {
    const expanded = expandHome(configPath, homeDir);
    const explicit = path.resolve(cwd, expanded);
    if (!existingFile(explicit)) {
      fail("CONFIG_NOT_FOUND", `explicit config file not found: ${explicit}`, 2);
    }
    return explicit;
  }

  const candidates = [];
  if (projectRoot) {
    const expandedRoot = expandHome(projectRoot, homeDir);
    candidates.push(path.join(path.resolve(cwd, expandedRoot), CONFIG_FILENAME));
  }
  candidates.push(path.join(homeDir, CONFIG_FILENAME));

  const found = candidates.find(existingFile);
  if (found) {
    return found;
  }
  fail(
    "CONFIG_NOT_FOUND",
    `config file not found; searched: ${candidates.join(", ")}`,
    2,
  );
}

export function loadConfig(configPath) {
  let source;
  try {
    source = fs.readFileSync(configPath, "utf8");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    fail("CONFIG_READ_FAILED", `cannot read config ${configPath}: ${message}`, 2);
  }

  let raw;
  try {
    raw = YAML.parse(source);
  } catch (error) {
    const position = error?.linePos?.[0];
    const location = position
      ? ` at line ${position.line}, column ${position.col}`
      : "";
    fail("CONFIG_INVALID", `cannot parse config ${configPath}${location}`, 2);
  }
  return validateConfig(raw);
}

export function redactSecrets(message, config) {
  let redacted = String(message);
  const physicalLocations = [];
  for (const storage of Object.values(config.storages)) {
    for (const secret of [storage.accessKeyId, storage.secretAccessKey, storage.sessionToken]) {
      if (secret) {
        redacted = redacted.split(secret).join("[REDACTED]");
      }
    }
    if (storage.endpoint) {
      physicalLocations.push([storage.endpoint, "[REDACTED_ENDPOINT]"]);
    }
    physicalLocations.push([storage.bucket, "[REDACTED_BUCKET]"]);
  }
  for (const database of Object.values(config.databases)) {
    for (const table of Object.values(database.tables)) {
      const storage = config.storages[table.storage];
      physicalLocations.push([
        `s3://${storage.bucket}/${table.prefix}/`,
        "s3://[REDACTED_ARCHIVE]/",
      ]);
      physicalLocations.push([table.prefix, "[REDACTED_PREFIX]"]);
    }
  }
  physicalLocations.sort((left, right) => right[0].length - left[0].length);
  for (const [physical, replacement] of physicalLocations) {
    if (physical) {
      redacted = redacted.split(physical).join(replacement);
    }
  }
  return redacted;
}
