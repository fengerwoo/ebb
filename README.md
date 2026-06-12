# ebb

> **ebb**（退潮）：数据像潮水一样从热库（MySQL）退向冷存储（S3 兼容对象存储），线上只保留热数据，冷数据通过 DuckDB 随时可查。

适用于持续增长的**追加型（append-only）日志表**：定时把增量备份为对象存储上的 Parquet 文件，校验归档完整后把线上过期数据分批安全删除，归档数据用 DuckDB SQL 统一查询。

## 特性

- **自动归档**：按固定周期（秒级可配）把 `id > 水位` 的增量写为 zstd 压缩的 Parquet，按天分区（`dt=YYYY-MM-DD`）；
- **自动瘦身**：归档校验通过后，线上仅保留最近一段时间的数据，分批删除，不锁表；
- **每日合并**：把当天产生的增量小文件合并为单个天级文件，保证查询性能；
- **无状态**：水位完全由对象存储上的文件名重建（`inc-{from_id}-{to_id}.parquet`），无本地状态、无元数据库，容器可随时强杀重启，不丢不重；
- **通用可配置**：任意 MySQL 库/表、多 job 并发；支持 AWS S3、阿里云 OSS、腾讯云 COS、MinIO 等 S3 兼容存储；
- **可查询**：内置 HTTP 查询 API（Bearer Token 认证、只读约束、超时与行数限制），或本地 `ebb query` 直查。

## 前置要求（对源表）

- 自增整数主键（游标列，默认 `id`）；
- 一个时间字段用于推导日期分区，类型可配：`unix_s` / `unix_ms` / `unix_us` / `datetime` / `timestamp`；
- 表为追加型：增量基于自增 id 游标，不感知 UPDATE / DELETE。

## 快速开始

```bash
cp config.yml.example config.yml   # 按注释填好数据源、对象存储与 job
docker compose up -d               # 入口即 ebb serve
docker logs -f ebb                 # 结构化 JSON 日志
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

## 首次接入：表里已有存量数据

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

## 存储布局

```
s3://bucket/{prefix}/
  dt=2026-06-10/data-1-120000.parquet        ← 天级合并文件
  dt=2026-06-11/data-120001-265000.parquet
  dt=2026-06-12/inc-265001-266200.parquet    ← 今天的增量，每轮一个
  dt=2026-06-12/inc-266201-267100.parquet
```

所有文件名携带 id 区间，**文件名即水位**：每轮开始 LIST 一次、取 `max(to_id)` 即可恢复进度。先上传、后（隐式）提交，同一水位重跑生成同名文件覆盖上传，任何时刻 kill -9 都不丢不重。

## 查询

天级文件与增量文件被通配符无差别覆盖，数据新鲜度即导出周期：

```sql
-- HTTP API（POST /query，Authorization: Bearer <key>）或 ebb query；
-- 每个 job 自动注册同名视图（默认取表名）
SELECT dt, count(*) FROM logs
WHERE dt BETWEEN '2026-06-01' AND '2026-06-12'
GROUP BY dt ORDER BY dt;
```

`WHERE dt = ...` 自动做分区裁剪。也可以把只读对象存储凭证发给可信使用方，对方本地 DuckDB / DBeaver 直查，不经过本服务：

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID '...', SECRET '...', ENDPOINT '...');
SELECT count(*) FROM read_parquet('s3://bucket/prefix/*/*.parquet', hive_partitioning=true);
```

## 配置

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

## 开发与测试

```bash
uv venv && uv pip install -e ".[dev]"
docker compose -f tests/docker-compose.test.yml up -d --wait   # 测试用 MySQL + MinIO
.venv/bin/python -m pytest
```

## 设计要点

- **调度器 + DuckDB 引擎**：读 MySQL（`mysql` 扩展原生 SQL 下推）、写 Parquet、上传 S3、合并、查询全部由 DuckDB 完成，Python 只做编排；
- **幂等**：导出按「分区日期连续段」切文件并按 id 顺序上传，已上传文件覆盖的 id 区间任何时刻都是从旧水位起的连续区间；合并走 临时文件 → 校验 → 改名 → 删源，重跑按游标列去重收敛；删除边界由「水位 + 保留期」实时推导，中断后重新推导继续；
- **防重叠**：APScheduler `max_instances=1` + `coalesce=True`，上一轮没结束就跳过本轮，下一轮自然从水位继续追。

## License

MIT
