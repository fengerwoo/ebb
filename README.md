# ebb

<p align="center">
  <b><a href="#中文">[ 中文</a></b> | <b><a href="#english">English ]</a></b>
</p>

> **ebb**（退潮）：数据像潮水一样从热库（MySQL）退向冷存储（S3 兼容对象存储），线上只保留热数据，冷数据通过 DuckDB 随时可查。

> **ebb**: data recedes like the tide — from the hot store (MySQL) to cold storage (S3-compatible object storage). Only hot data stays online; cold data remains queryable anytime via DuckDB.

---

## 中文

适用于持续增长的**追加型（append-only）日志表**：定时把增量备份为对象存储上的 Parquet 文件，校验归档完整后把线上过期数据分批安全删除，归档数据用 DuckDB SQL 统一查询。

### 特性

- **自动归档**：按固定周期（秒级可配）把 `id > 水位` 的增量写为 zstd 压缩的 Parquet，按天分区（`dt=YYYY-MM-DD`）；
- **自动瘦身**：归档校验通过后，线上仅保留最近一段时间的数据，分批删除，不锁表；
- **每日合并**：把当天产生的增量小文件合并为单个天级文件，保证查询性能；
- **无状态**：水位完全由对象存储上的文件名重建（`inc-{from_id}-{to_id}.parquet`），无本地状态、无元数据库，容器可随时强杀重启，不丢不重；
- **通用可配置**：任意 MySQL 库/表、多 job 并发；支持 AWS S3、阿里云 OSS、腾讯云 COS、MinIO 等 S3 兼容存储；
- **可查询**：内置 HTTP 查询 API（Bearer Token 认证、只读约束、超时与行数限制），或本地 `ebb query` 直查。

### 前置要求（对源表）

- 自增整数主键（游标列，默认 `id`）；
- 一个时间字段用于推导日期分区，类型可配：`unix_s` / `unix_ms` / `unix_us` / `datetime` / `timestamp`；
- 表为追加型：增量基于自增 id 游标，不感知 UPDATE / DELETE。

### 安装与快速开始

推荐直接使用 GHCR 镜像（多架构：amd64 / arm64），无需 Python 环境：

```bash
# 1. 拉取镜像
docker pull ghcr.io/fengerwoo/ebb:latest

# 2. 导出样例配置，按注释填好数据源、对象存储与 job
docker run --rm --entrypoint cat ghcr.io/fengerwoo/ebb /app/config.yml.example > config.yml
vim config.yml

# 3. 体检：验证 MySQL / 对象存储连通性与表结构
docker run --rm -v ./config.yml:/etc/ebb/config.yml:ro ghcr.io/fengerwoo/ebb check
```

体检通过后，写一份 `docker-compose.yml` 常驻运行：

```yaml
services:
  ebb:
    image: ghcr.io/fengerwoo/ebb:latest
    container_name: ebb
    restart: unless-stopped
    volumes:
      - ./config.yml:/etc/ebb/config.yml:ro
    ports:
      - "18761:18761"   # HTTP 查询 API；未启用或不对外可删掉
```

```bash
docker compose up -d               # 入口即 ebb serve
docker logs -f ebb                 # 结构化 JSON 日志
```

> 提示：MySQL 跑在宿主机时，容器内请用 `host.docker.internal`（Mac/Windows）或 host 网络模式（Linux）访问；修改 config.yml 后 `docker restart ebb` 生效。

也可以从源码构建：

```bash
git clone https://github.com/fengerwoo/ebb.git && cd ebb
cp config.yml.example config.yml   # 按注释填好
# docker-compose.yml 中取消 build: . 的注释
docker compose up -d --build
```

日常操作：

```bash
docker exec ebb ebb check          # 新加 job 后先体检（连接、表结构、读写权限）
docker exec ebb ebb status         # 水位、落后行数、追平估算（不依赖 serve）
docker exec ebb ebb ps             # 正在执行的任务与进度
docker exec ebb ebb run --job xxx --once --dry-run   # 手动试跑一轮
docker exec ebb ebb backfill --job xxx               # 存量回填（缺省：线上最早一天 ~ 昨天；可用 --from/--to 指定）
docker exec ebb ebb purge --job xxx --dry-run        # 预览将删除的区间
docker exec ebb ebb query "SELECT count(*) FROM logs WHERE dt = '2026-06-11'"
```

测试新表的推荐流程：`check` → `status` → `run --once --dry-run` → `run --once` → 加入 `serve` 常驻。

### 首次接入：表里已有存量数据

新接入的表如果已经积累了大量历史数据（比如几个月、几千万行），不要直接交给增量导出去追，按下面顺序做：

1. **确认时间列有索引**。回填按天切片查询（`WHERE time >= 当天 AND < 次日`），没有索引就是每天一次全表扫描；
2. **保持 `schedule.enabled: false`，体检**；
3. **低峰期回填**。缺省区间就是「线上最早一天 ～ 昨天」，今天的数据留给增量导出接力，不会混写分区。按天切片、中断随时重跑（已有数据文件的分区自动跳过）；
4. **首次删除单独对待**：先 dry-run 预览，确认后在低峰期手动执行。首次会删除全部已归档的过期数据（量可能很大，分批执行、注意 binlog 与主从延迟），之后每天就只有增量的量了；
5. **开启调度**：把 `schedule.enabled` 改为 `true` 加入 `serve`。启动后立即跑一轮增量导出，自动追平回填之后新增的数据（单轮上限 `export_rows`，追不平下一轮继续）。

```bash
docker exec ebb ebb check --job xxx                  # 1. 体检：连接、表结构、读写权限
docker exec ebb ebb status --job xxx                 #    看水位与落后行数
docker exec ebb ebb backfill --job xxx               # 2. 低峰期回填（最早一天 ~ 昨天）
docker exec ebb ebb purge --job xxx --dry-run        # 3. 预览首次删除的区间与行数
docker exec ebb ebb purge --job xxx                  #    低峰期手动跑掉首次大删除
# 4. config.yml 里把该 job 的 schedule.enabled 改为 true，重启容器生效
docker restart ebb
docker exec ebb ebb ps                               #    确认调度已开始追增量
```

也就是说：回填成功 → （可选但推荐）手动跑掉首次大删除 → 打开 `schedule.enabled` 即完成接入。

### 存储布局

```
s3://bucket/{prefix}/
  dt=2026-06-10/data-1-120000.parquet        ← 天级合并文件
  dt=2026-06-11/data-120001-265000.parquet
  dt=2026-06-12/inc-265001-266200.parquet    ← 今天的增量，每轮一个
  dt=2026-06-12/inc-266201-267100.parquet
```

所有文件名携带 id 区间，**文件名即水位**：每轮开始 LIST 一次、取 `max(to_id)` 即可恢复进度。先上传、后（隐式）提交，同一水位重跑生成同名文件覆盖上传，任何时刻 kill -9 都不丢不重。

### 查询

天级文件与增量文件被通配符无差别覆盖，数据新鲜度即导出周期：

```sql
-- HTTP API（POST /query，Authorization: Bearer <key>）或 ebb query；
-- 每个 job 自动注册同名视图（默认取表名）
SELECT dt, count(*) FROM logs
WHERE dt BETWEEN '2026-06-01' AND '2026-06-12'
GROUP BY dt ORDER BY dt;
```

`WHERE dt = ...` 自动做分区裁剪。

支持 Skills 的 Agent 可以使用仓库内置的只读查询 Skill，直接发送这一句话：

> 请安装 https://github.com/fengerwoo/ebb/tree/main/skills/ebb-query-data ，安装后阅读并遵循 SKILL.md；若尚无配置，先询问我要使用项目级、全局还是自定义路径，只复制示例配置并 chmod 600，由我自行填写凭据，不要读取或打印真实配置。

也可以把只读对象存储凭证发给可信使用方，对方本地 DuckDB / DBeaver 直查，不经过本服务：

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID '...', SECRET '...', ENDPOINT '...');
SELECT count(*) FROM read_parquet('s3://bucket/prefix/*/*.parquet', hive_partitioning=true);
```

### 配置

见 [config.yml.example](config.yml.example)，要点：

| 配置 | 说明 |
|---|---|
| `jobs[].schedule.interval_seconds` | 增量导出周期（秒） |
| `jobs[].schedule.compact_at` | 每日合并时刻（job 时区），合并后顺带执行 purge |
| `jobs[].schedule.purge_interval_seconds` | 设置后 purge 按该间隔独立调度，每日任务只做合并；保留期短于一天时必配，否则每天只删一次 |
| `jobs[].retention.online_retain_seconds` | 线上保留时长（秒） |
| `jobs[].retention.verify_before_delete` | 删除前对比 MySQL 与 Parquet 行数 + id 和 |
| `jobs[].batch.export_rows` | 单轮导出行数上限（追不平下一轮继续） |
| `jobs[].batch.safety_lag_seconds` | 只导出早于 now − N 秒的行，规避自增 id 提交乱序漏读 |

### 开发与测试

```bash
uv venv && uv pip install -e ".[dev]"
docker compose -f tests/docker-compose.test.yml up -d --wait   # 测试用 MySQL + MinIO
.venv/bin/python -m pytest
```

### 设计要点

- **调度器 + DuckDB 引擎**：读 MySQL（`mysql` 扩展原生 SQL 下推）、写 Parquet、上传 S3、合并、查询全部由 DuckDB 完成，Python 只做编排；
- **幂等**：导出按「分区日期连续段」切文件并按 id 顺序上传，已上传文件覆盖的 id 区间任何时刻都是从旧水位起的连续区间；合并走 临时文件 → 校验 → 改名 → 删源，重跑按游标列去重收敛；删除边界由「水位 + 保留期」实时推导，中断后重新推导继续；
- **防重叠**：APScheduler `max_instances=1` + `coalesce=True`，上一轮没结束就跳过本轮，下一轮自然从水位继续追。

---

## English

For ever-growing **append-only log tables**: periodically archive increments as Parquet files on object storage, verify the archive is complete, then safely delete expired rows online in batches — and query everything archived with DuckDB SQL.

### Features

- **Automatic archiving**: at a fixed interval (configurable down to seconds), exports rows with `id > watermark` as zstd-compressed Parquet, partitioned by day (`dt=YYYY-MM-DD`);
- **Automatic slimming**: once archive verification passes, only a recent window of data stays online; expired rows are deleted in batches, without locking the table;
- **Daily compaction**: merges the day's small incremental files into a single daily file to keep query performance;
- **Stateless**: the watermark is fully rebuilt from file names on object storage (`inc-{from_id}-{to_id}.parquet`) — no local state, no metadata database; the container can be force-killed and restarted at any time with no data loss or duplication;
- **Generic and configurable**: any MySQL database/table, multiple concurrent jobs; supports AWS S3, Alibaba Cloud OSS, Tencent Cloud COS, MinIO and other S3-compatible storage;
- **Queryable**: built-in HTTP query API (Bearer Token auth, read-only constraints, timeout and row limits), or query locally with `ebb query`.

### Prerequisites (for the source table)

- An auto-increment integer primary key (the cursor column, default `id`);
- A time column used to derive the date partition; its type is configurable: `unix_s` / `unix_ms` / `unix_us` / `datetime` / `timestamp`;
- The table is append-only: incremental export follows the auto-increment id cursor and is unaware of UPDATE / DELETE.

### Install & Quick Start

Recommended: use the GHCR image directly (multi-arch: amd64 / arm64), no Python environment needed:

```bash
# 1. Pull the image
docker pull ghcr.io/fengerwoo/ebb:latest

# 2. Export the sample config; fill in sources, object storage and jobs per the comments
docker run --rm --entrypoint cat ghcr.io/fengerwoo/ebb /app/config.yml.example > config.yml
vim config.yml

# 3. Health check: verify MySQL / object storage connectivity and table schema
docker run --rm -v ./config.yml:/etc/ebb/config.yml:ro ghcr.io/fengerwoo/ebb check
```

Once the health check passes, write a `docker-compose.yml` and run it as a daemon:

```yaml
services:
  ebb:
    image: ghcr.io/fengerwoo/ebb:latest
    container_name: ebb
    restart: unless-stopped
    volumes:
      - ./config.yml:/etc/ebb/config.yml:ro
    ports:
      - "18761:18761"   # HTTP query API; remove if not enabled or not exposed
```

```bash
docker compose up -d               # the entrypoint is `ebb serve`
docker logs -f ebb                 # structured JSON logs
```

> Tip: if MySQL runs on the host, use `host.docker.internal` (Mac/Windows) or host network mode (Linux) from inside the container; after editing config.yml, apply with `docker restart ebb`.

Or build from source:

```bash
git clone https://github.com/fengerwoo/ebb.git && cd ebb
cp config.yml.example config.yml   # fill in per the comments
# uncomment `build: .` in docker-compose.yml
docker compose up -d --build
```

Day-to-day operations:

```bash
docker exec ebb ebb check          # health check after adding a job (connectivity, schema, permissions)
docker exec ebb ebb status         # watermark, row lag, catch-up estimate (independent of serve)
docker exec ebb ebb ps             # running tasks and their progress
docker exec ebb ebb run --job xxx --once --dry-run   # manual trial round
docker exec ebb ebb backfill --job xxx               # backfill history (default: earliest day online ~ yesterday; or use --from/--to)
docker exec ebb ebb purge --job xxx --dry-run        # preview the range to be deleted
docker exec ebb ebb query "SELECT count(*) FROM logs WHERE dt = '2026-06-11'"
```

Recommended flow for a new table: `check` → `status` → `run --once --dry-run` → `run --once` → add to `serve`.

### First-time onboarding: the table already has historical data

If the table being onboarded has already accumulated a lot of history (say months of data, tens of millions of rows), don't let incremental export chase it directly. Follow this order:

1. **Make sure the time column is indexed.** Backfill queries slice by day (`WHERE time >= day AND < next day`); without an index, every day is a full table scan;
2. **Keep `schedule.enabled: false` and run the health check**;
3. **Backfill during off-peak hours.** The default range is "earliest day online ~ yesterday"; today's data is left for incremental export to take over, so partitions never mix. Sliced by day and safe to re-run after interruption (partitions that already have data files are skipped automatically);
4. **Treat the first purge separately**: preview with dry-run first, then run it manually during off-peak hours. The first purge deletes all archived expired data (possibly a lot — it deletes in batches; watch binlog volume and replication lag). After that, only the daily increment remains;
5. **Enable scheduling**: set `schedule.enabled` to `true` and add the job to `serve`. On startup it immediately runs one round of incremental export, automatically catching up the data added since the backfill (per-round cap `export_rows`; if it can't catch up, the next round continues).

```bash
docker exec ebb ebb check --job xxx                  # 1. health check: connectivity, schema, permissions
docker exec ebb ebb status --job xxx                 #    watermark and row lag
docker exec ebb ebb backfill --job xxx               # 2. off-peak backfill (earliest day ~ yesterday)
docker exec ebb ebb purge --job xxx --dry-run        # 3. preview the first purge range and row count
docker exec ebb ebb purge --job xxx                  #    run the first big purge manually off-peak
# 4. set this job's schedule.enabled to true in config.yml, then restart to apply
docker restart ebb
docker exec ebb ebb ps                               #    confirm scheduling has started catching up
```

In short: backfill succeeds → (optional but recommended) manually run the first big purge → turn on `schedule.enabled`, and onboarding is done.

### Storage Layout

```
s3://bucket/{prefix}/
  dt=2026-06-10/data-1-120000.parquet        ← daily compacted file
  dt=2026-06-11/data-120001-265000.parquet
  dt=2026-06-12/inc-265001-266200.parquet    ← today's increments, one per round
  dt=2026-06-12/inc-266201-267100.parquet
```

Every file name carries an id range — **the file name is the watermark**: at the start of each round, one LIST plus `max(to_id)` restores progress. Upload first, then (implicitly) commit; re-running at the same watermark produces identically named files that overwrite on upload, so a kill -9 at any moment loses nothing and duplicates nothing.

### Query

Daily files and incremental files are covered uniformly by the wildcard; data freshness equals the export interval:

```sql
-- HTTP API (POST /query, Authorization: Bearer <key>) or `ebb query`;
-- each job auto-registers a view of the same name (defaults to the table name)
SELECT dt, count(*) FROM logs
WHERE dt BETWEEN '2026-06-01' AND '2026-06-12'
GROUP BY dt ORDER BY dt;
```

`WHERE dt = ...` gets partition pruning automatically.

For an agent that supports Skills, send this single instruction to install the repository's bundled read-only query Skill:

> Install https://github.com/fengerwoo/ebb/tree/main/skills/ebb-query-data, then read and follow SKILL.md; if no configuration exists, first ask whether I want a project-level, user-global, or custom path, copy only the example configuration there and run chmod 600, then let me enter the credentials myself without reading or printing the real configuration.

You can also hand read-only object storage credentials to trusted consumers, who query directly with local DuckDB / DBeaver, bypassing this service entirely:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID '...', SECRET '...', ENDPOINT '...');
SELECT count(*) FROM read_parquet('s3://bucket/prefix/*/*.parquet', hive_partitioning=true);
```

### Configuration

See [config.yml.example](config.yml.example). Highlights:

| Option | Description |
|---|---|
| `jobs[].schedule.interval_seconds` | Incremental export interval (seconds) |
| `jobs[].schedule.compact_at` | Daily compaction time (job timezone); purge runs right after compaction |
| `jobs[].schedule.purge_interval_seconds` | When set, purge is scheduled independently at this interval and the daily task only compacts; required when retention is shorter than one day, otherwise deletion happens only once per day |
| `jobs[].retention.online_retain_seconds` | Online retention window (seconds) |
| `jobs[].retention.verify_before_delete` | Before deleting, compare row count + id sum between MySQL and Parquet |
| `jobs[].batch.export_rows` | Per-round export row cap (the next round continues if behind) |
| `jobs[].batch.safety_lag_seconds` | Only export rows older than now − N seconds, avoiding misses from out-of-order auto-increment id commits |

### Development & Testing

```bash
uv venv && uv pip install -e ".[dev]"
docker compose -f tests/docker-compose.test.yml up -d --wait   # MySQL + MinIO for tests
.venv/bin/python -m pytest
```

### Design Notes

- **Scheduler + DuckDB engine**: reading MySQL (native SQL pushdown via the `mysql` extension), writing Parquet, uploading to S3, compaction and querying are all done by DuckDB; Python only orchestrates;
- **Idempotency**: export splits files by contiguous runs of partition dates and uploads in id order, so the ids covered by uploaded files always form a contiguous range starting at the old watermark; compaction goes temp file → verify → rename → delete sources, and re-runs converge by deduplicating on the cursor column; purge boundaries are derived on the fly from "watermark + retention", so after an interruption it simply re-derives and continues;
- **No overlap**: APScheduler `max_instances=1` + `coalesce=True` — if the previous round hasn't finished, this round is skipped, and the next round naturally continues from the watermark.

## License

MIT
