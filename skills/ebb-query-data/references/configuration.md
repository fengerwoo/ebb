# Configuration Reference

Use this reference when creating or troubleshooting `ebb-query-data.yaml`.

## Resolution

Resolve configuration in this order:

1. Use `--config <path>` exclusively when supplied. Fail if that file does not exist.
2. Check `<project-root>/ebb-query-data.yaml` when `--project-root` is supplied.
3. Fall back to `~/ebb-query-data.yaml`.

Resolve a relative `--output` path against the supplied project root. Resolve a relative explicit
`--config` path against the process working directory.

## Model

- Treat `storages` as physical S3-compatible buckets and credentials.
- Treat each `databases` entry as a logical SQL namespace for related archive tables.
- Treat each table as a stable SQL name mapped to one Ebb archive prefix.
- Read Ebb objects from `s3://<bucket>/<prefix>/dt=*/*.parquet`.
- Never expose a bucket or object path as a SQL identifier.

Use the bundled `../ebb-query-data.example.yaml` as the canonical example.

## Storage Fields

| Field | Required | Meaning |
| --- | --- | --- |
| `type` | No | Must be `s3`; defaults to `s3`. |
| `endpoint` | No | S3-compatible host, with or without `http(s)://`; empty uses AWS defaults. |
| `bucket` | Yes | Physical bucket name. |
| `access_key_id` | Yes | Credential stored directly in this local config. |
| `secret_access_key` | Yes | Credential stored directly in this local config. |
| `session_token` | No | Temporary credential token. |
| `region` | No | Provider region. |
| `url_style` | No | `vhost` or `path`; defaults to `vhost`. |
| `use_ssl` | No | Defaults to `true`, or follows an endpoint URL scheme. |

Set the real config permissions to owner-only where the operating system supports it:

```bash
chmod 600 <project-root>/ebb-query-data.yaml
```

Do not read or print the real config while using the Skill. The CLI loads it directly and redacts
known credential values from DuckDB errors.

The first remote query may download DuckDB's platform-specific `httpfs` extension. The CLI stores
it under `$XDG_CACHE_HOME/ebb-query-data/duckdb-extensions` when set, otherwise under the operating
system's user cache directory. This keeps globally installed, read-only Skill directories usable.

## Query Fields

`query.default_database` supplies the database for `SHOW TABLES`, unqualified `DESCRIBE`, and
unqualified tables in `SELECT`. Prefer fully qualified `database.table` names in generated SQL.

`query.max_rows` is a hard ceiling. A smaller `--max-rows` is allowed; a larger value is capped.
`query.max_result_bytes` limits the estimated serialized result size and defaults to 50 MB.
`query.timeout_seconds` defaults to 900 seconds and is the hard ceiling for `--timeout-seconds`.
Pass `--timeout-seconds 900` for remote schema reads, data queries, and exports; set the config field
explicitly when the project needs a stricter runtime policy.

## Supported SQL

Use these metadata commands:

```sql
SHOW DATABASES;
SHOW TABLES FROM analytics;
DESCRIBE analytics.events;
SHOW COLUMNS FROM analytics.events;
```

`DESCRIBE` and `SHOW COLUMNS` report the highest-watermark Parquet object's schema. This keeps
schema discovery bounded even when a table has many historical files. Multi-day data queries use
`union_by_name` across the files selected by the logical table view.

Run exactly one DuckDB `SELECT`, `WITH`, or `FROM` statement for data queries. The CLI rejects
DDL, DML, `COPY`, `ATTACH`, `INSTALL`, `LOAD`, `SET`, multiple statements, and unsupported `SHOW`
forms. It also rejects user-supplied table functions, including direct `read_parquet` or `read_csv`
calls. It parses SQL with DuckDB, registers only the configured logical tables referenced by that
query, and then disables local and arbitrary HTTP filesystems after creating scoped S3 views.

Always filter large archive tables by the Hive partition column `dt` when the request permits it:

```sql
SELECT event_type, count(*) AS event_count
FROM analytics.events
WHERE dt BETWEEN '2026-07-01' AND '2026-07-22'
GROUP BY event_type
ORDER BY event_count DESC;
```

For a direct single-table query, an explicit top-level `dt = ...`, `dt BETWEEN ... AND ...`, or
bounded `dt >= ... AND dt <= ...` predicate lets the CLI register only those date partitions before
DuckDB performs schema union. It constructs exact `dt=YYYY-MM-DD/*.parquet` globs and lists only the
requested date directories, so missing dates inside a range are harmless and a wholly empty range
returns an empty result with the table's latest schema.
Ambiguous predicates, `OR`, repeated table references, subqueries, and ranges over 400 days
deliberately fall back to the complete logical table.

Use a small projected `LIMIT 1` query against a known populated date to test data access. Do not use
`count(*)` for connectivity checks because aggregates must inspect all selected files. A process
runner may yield control while the isolated query worker is still active; continue polling the same
process until it returns the JSON result or a structured timeout.

For an exact single-day predicate, the CLI orders files by Ebb watermark and uses the latest-file
schema to avoid reading every incremental file footer twice. Queries spanning multiple days retain
`union_by_name` so schema changes across historical partitions remain queryable. If a table's schema
changes within one day, use a CTE/subquery wrapper to deliberately fall back to the complete logical
table and full schema union.

## Output

Use JSON by default. JSON preserves column metadata and returns rows as objects. DuckDB `BIGINT`,
`HUGEINT`, `DECIMAL`, temporal, UUID, and binary values use lossless JSON-safe strings.

The query worker uses bounded DuckDB and JavaScript heaps. `max_result_bytes` is checked before
rendering JSON or CSV; reduce selected columns, shorten text fields, or aggregate when the CLI
returns `RESULT_TOO_LARGE`.

Use `--format csv` only for export or when the user requests CSV. CSV writes a header by default;
pass `--no-header` to omit it. Use `--output <path>` for files. Without `--output`, write only data
to stdout and diagnostics to stderr. Output paths must be relative, remain inside the project root,
and not already exist.
