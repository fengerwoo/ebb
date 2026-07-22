import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  CONFIG_FILENAME,
  loadConfig,
  resolveConfigPath,
  validateConfig,
} from "../scripts/lib/config.mjs";

function validConfig() {
  return {
    version: 1,
    storages: {
      archive: {
        type: "s3",
        endpoint: "https://storage.example.com",
        bucket: "archive-bucket",
        access_key_id: "key-id",
        secret_access_key: "key-secret",
        region: "us-east-1",
        url_style: "path",
      },
    },
    databases: {
      analytics: {
        tables: {
          events: {
            storage: "archive",
            prefix: "/analytics/events/",
          },
        },
      },
    },
    query: {
      default_database: "analytics",
      max_rows: 25,
      max_result_bytes: 50000,
      timeout_seconds: 5,
    },
  };
}

test("validateConfig normalizes storage and table settings", () => {
  const config = validateConfig(validConfig());
  assert.equal(config.storages.archive.endpoint, "storage.example.com");
  assert.equal(config.storages.archive.useSsl, true);
  assert.equal(config.databases.analytics.tables.events.prefix, "analytics/events");
  assert.equal(config.query.defaultDatabase, "analytics");
  assert.equal(config.query.maxRows, 25);
  assert.equal(config.query.maxResultBytes, 50000);
});

test("validateConfig defaults query timeouts for remote archives", () => {
  const input = validConfig();
  delete input.query.timeout_seconds;
  const config = validateConfig(input);
  assert.equal(config.query.timeoutSeconds, 900);
});

test("validateConfig rejects unknown fields and unsafe prefixes", () => {
  const unknown = validConfig();
  unknown.query.max_row = 10;
  assert.throws(() => validateConfig(unknown), /query\.max_row: unknown field/);

  const wildcard = validConfig();
  wildcard.databases.analytics.tables.events.prefix = "analytics/*";
  assert.throws(() => validateConfig(wildcard), /must not contain wildcard/);
});

test("validateConfig rejects unknown references and reserved database names", () => {
  const missingStorage = validConfig();
  missingStorage.databases.analytics.tables.events.storage = "missing";
  assert.throws(() => validateConfig(missingStorage), /references unknown storage/);

  const reserved = validConfig();
  reserved.databases.main = reserved.databases.analytics;
  delete reserved.databases.analytics;
  reserved.query.default_database = "main";
  assert.throws(() => validateConfig(reserved), /reserved database name/);
});

test("validateConfig rejects SQL names that differ only by case", () => {
  const databases = validConfig();
  databases.databases.ANALYTICS = databases.databases.analytics;
  assert.throws(() => validateConfig(databases), /conflicts case-insensitively/);

  const tables = validConfig();
  tables.databases.analytics.tables.EVENTS = tables.databases.analytics.tables.events;
  assert.throws(() => validateConfig(tables), /conflicts case-insensitively/);
});

test("resolveConfigPath uses explicit, project, then home precedence", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ebb-query-config-"));
  const project = path.join(root, "project");
  const home = path.join(root, "home");
  fs.mkdirSync(project);
  fs.mkdirSync(home);
  const projectConfig = path.join(project, CONFIG_FILENAME);
  const homeConfig = path.join(home, CONFIG_FILENAME);
  const explicitConfig = path.join(root, "explicit.yaml");
  fs.writeFileSync(projectConfig, "version: 1\n");
  fs.writeFileSync(homeConfig, "version: 1\n");
  fs.writeFileSync(explicitConfig, "version: 1\n");

  assert.equal(
    resolveConfigPath({ configPath: explicitConfig, projectRoot: project, homeDir: home }),
    explicitConfig,
  );
  assert.equal(resolveConfigPath({ projectRoot: project, homeDir: home }), projectConfig);
  fs.unlinkSync(projectConfig);
  assert.equal(resolveConfigPath({ projectRoot: project, homeDir: home }), homeConfig);
});

test("an explicit missing config never falls back", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ebb-query-config-"));
  fs.writeFileSync(path.join(root, CONFIG_FILENAME), "version: 1\n");
  assert.throws(
    () => resolveConfigPath({ configPath: "missing.yaml", homeDir: root, cwd: root }),
    /explicit config file not found/,
  );
});

test("YAML parse failures never echo source lines containing secrets", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ebb-query-config-"));
  const configPath = path.join(root, CONFIG_FILENAME);
  const secret = "SUPER_SECRET_MUST_NOT_LEAK";
  fs.writeFileSync(configPath, `version: 1\nsecret_access_key: ${secret}: invalid\n`);

  assert.throws(
    () => loadConfig(configPath),
    (error) => {
      assert.equal(error.code, "CONFIG_INVALID");
      assert.doesNotMatch(error.message, new RegExp(secret));
      assert.match(error.message, /line 2, column/);
      return true;
    },
  );
});

test("redaction removes credentials and physical storage locations", async () => {
  const { redactSecrets } = await import("../scripts/lib/config.mjs");
  const config = validateConfig(validConfig());
  const storage = config.storages.archive;
  const table = config.databases.analytics.tables.events;
  const message =
    `${storage.accessKeyId} ${storage.secretAccessKey} ${storage.endpoint} ` +
    `s3://${storage.bucket}/${table.prefix}/dt=*/data.parquet`;
  const redacted = redactSecrets(message, config);
  for (const physical of [
    storage.accessKeyId,
    storage.secretAccessKey,
    storage.endpoint,
    storage.bucket,
    table.prefix,
  ]) {
    assert.equal(redacted.includes(physical), false);
  }
});
