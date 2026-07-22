---
name: ebb-query-data
description: Query Ebb Parquet archives in S3-compatible storage through a safe logical database.table SQL catalog. Use when an agent needs to discover archived datasets, inspect schemas, run read-only analytical SQL over Ebb data, or export query results as JSON or CSV.
---

# Ebb Query Data

Use the bundled Node CLI instead of reading object-storage credentials or constructing raw S3 URLs.

## Prepare

1. Determine the active project root from the current workspace.
2. Use `<project-root>/ebb-query-data.yaml` when present; otherwise let the CLI fall back to
   `~/ebb-query-data.yaml`.
3. Never open, print, quote, or summarize the real configuration because it contains credentials.
4. Run `npm ci --omit=dev` in this Skill directory only when `node_modules` is absent. State that
   this Skill-required installation is occurring before running it.
5. Expect the first remote query to install DuckDB's matching `httpfs` extension into the user's
   cache.
6. Read [references/configuration.md](references/configuration.md) only when configuring a project,
   troubleshooting, or needing the complete SQL and output contract.
7. Pass `--timeout-seconds 900` for commands that open remote Parquet data (`DESCRIBE`,
   `SELECT`/`WITH`/`FROM`, and CSV exports). The CLI caps this at the configured hard limit. Pure
   catalog commands such as `SHOW DATABASES` and `SHOW TABLES` do not need the longer timeout.

## Discover Before Querying

Run the CLI with an explicit project root. Start with metadata when the requested database, table,
or columns are uncertain:

```bash
node <skill-root>/scripts/ebb-query-data.mjs \
  --project-root <project-root> \
  --sql "SHOW DATABASES;"
```

Then inspect the relevant table:

```bash
node <skill-root>/scripts/ebb-query-data.mjs \
  --project-root <project-root> \
  --sql "SHOW TABLES FROM analytics;"

node <skill-root>/scripts/ebb-query-data.mjs \
  --project-root <project-root> \
  --timeout-seconds 900 \
  --sql "DESCRIBE analytics.events;"
```

`DESCRIBE` reads the schema from the most recent Parquet object so discovery does not scan every
historical file footer.

Do not infer logical names from bucket paths. Use only names returned by the catalog.

## Query

Generate exactly one read-only DuckDB `SELECT`, `WITH`, or `FROM` statement. Fully qualify tables
as `database.table`. Add a `dt` predicate for bounded time ranges and add a sensible SQL `LIMIT`
for detail queries even though the CLI enforces a hard result cap. Use configured logical tables;
the CLI rejects direct table functions such as `read_parquet`, `read_csv`, and `range`.

To verify data access, select a few small columns from a known populated date with `LIMIT 1`. Do not
use `count(*)` as a connectivity probe: it must scan all selected Parquet files, while `LIMIT 1`
can stop after the first row. Remote storage setup and object listing happen before either query
returns rows.

```bash
node <skill-root>/scripts/ebb-query-data.mjs \
  --project-root <project-root> \
  --timeout-seconds 900 \
  --sql "SELECT event_type, count(*) AS event_count FROM analytics.events WHERE dt = '2026-07-22' GROUP BY event_type ORDER BY event_count DESC;"
```

Treat JSON as the default agent-facing format. Use `rows`, `columns`, `rowCount`, `truncated`, and
`elapsedMs` from the result envelope. Tell the user when `truncated` is true rather than treating the
partial result as complete. Treat `RESULT_TOO_LARGE` as a request to select fewer/smaller columns or
aggregate the result; do not bypass the configured byte ceiling.

A remote query can outlive an execution tool's initial yield window. When the command runner
returns a live session instead of an exit code, keep polling that same session until the CLI emits
a result or structured error. Do not launch a duplicate query or report a timeout based only on the
runner's yield.

## Export CSV

Use CSV only when requested. Write to a project-relative file with `--output`; the CLI returns a JSON
summary on stdout while writing only CSV data to the file. Choose a new path inside the project;
the CLI rejects absolute paths, traversal outside the project, symlink escapes, and overwrites.

```bash
node <skill-root>/scripts/ebb-query-data.mjs \
  --project-root <project-root> \
  --timeout-seconds 900 \
  --format csv \
  --output query-result.csv \
  --sql "SELECT * FROM analytics.events WHERE dt = '2026-07-22';"
```

Without `--output`, CSV data goes to stdout. Pass `--no-header` only when the user requests it.

## Handle Failures

Read structured errors from stderr and report the error code and useful message without exposing
credentials. Do not bypass rejected SQL with raw DuckDB, object-storage commands, direct Parquet
readers, or a different script. Ask for configuration correction when the CLI reports missing
databases, tables, archives, or credentials.

On `QUERY_TIMEOUT`, narrow the `dt` range before retrying. For composable multi-day aggregates, run
one bounded day per CLI invocation and combine daily results carefully; do not average averages or
sum distinct counts. Increasing `--timeout-seconds` may have no effect because the configured value
is a hard cap. If a known populated single-day `LIMIT 1` query times out, report storage/listing
latency rather than retrying the same SQL repeatedly.
