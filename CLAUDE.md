# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

ebb：把 MySQL 追加型（append-only）日志表定时归档为 S3 兼容对象存储上的 Parquet 文件，校验通过后分批删除线上过期数据，归档数据用 DuckDB 统一查询。Python 3.11+，CLI 入口 `ebb = ebb.cli:main`（click）。完整设计文档见 `docs/DESIGN.md`。

## 常用命令

```bash
# 开发环境
uv venv && uv pip install -e ".[dev]"

# 测试（集成测试，需要本机 docker：会起 MySQL 8 + MinIO 并保留复用）
docker compose -f tests/docker-compose.test.yml up -d --wait
.venv/bin/python -m pytest                      # 全部测试
.venv/bin/python -m pytest tests/test_export.py            # 单个文件
.venv/bin/python -m pytest tests/test_export.py::test_xxx  # 单个测试
```

测试基础设施在 `tests/conftest.py`：MySQL 端口 33061、MinIO 端口 39001，session 级 fixture 自动起容器；每个测试用独立表名/前缀（`uniq` fixture）互不干扰。`make_config()` / `create_log_table()` / `insert_rows()` 是构造测试场景的标准入口。

无 lint/format 工具配置。

## 核心架构

**调度器 + DuckDB 引擎**：搬数据的重活（读 MySQL、写 Parquet、上传 S3、合并、查询）全部由 DuckDB 的 `mysql` + `httpfs` 扩展完成，Python 只做编排。

**水位 = 文件名（最核心的设计）**：系统完全无状态，无元数据库。所有数据文件名携带 id 区间（`inc-{from_id}-{to_id}.parquet` / `data-{from_id}-{to_id}.parquet`），每轮开始 S3 LIST 一次、取 `max(to_id)` 即恢复水位。先上传、后（隐式）提交，同一水位重跑生成同名文件覆盖上传，任何时刻 kill -9 都不丢不重。修改导出/合并/回填逻辑时必须维持这一不变量：**已上传文件覆盖的 id 必须始终是从旧水位起的连续区间**（导出按「分区日期连续段」切文件并按 id 顺序上传就是为此）。

**控制面 / 数据面分离**：

- 数据面（批量读写）走 DuckDB：`engine.py`（连接构造、S3 secret、MySQL ATTACH、`mysql_query` 原生 SQL 下推）；
- 控制面（轻量操作）走原生客户端：`s3util.py`（boto3：LIST/HEAD/改名/删除）、`mysqlutil.py`（pymysql：计数、最大 id、分批 DELETE）。

**生命周期管道**（每个 job 独立）：

- `export.py` — 增量导出，`id > 水位` 拉取，`safety_lag_seconds` 规避自增 id 提交乱序漏读；
- `compact.py` — 每日合并：写 `tmp-` 临时对象 → 校验行数 → 改名 `data-` → 删源文件；中断重跑按游标列去重收敛；
- `purge.py` — 校验后删除：边界由「水位 + 保留期」实时推导（不持久化进度），对比 MySQL 与 Parquet 行数 + id 和后分批 DELETE；
- `backfill.py` — 存量回填，按天切片，直接写 data 文件；已有数据的分区默认跳过。会抬高水位，应先回填后开增量；
- `scheduler.py` — `ebb serve` 常驻：APScheduler（`max_instances=1` + `coalesce=True` 防重叠），SIGTERM 优雅退出。

**两个独立的 FastAPI 应用**（`api.py`）：对外查询 API（`POST /query`，Bearer Token，可选启用）与仅回环地址的管理端点（`GET /admin/jobs`，供 `ebb ps` 读取 `registry.py` 内存注册表）。查询执行在 `queryservice.py`：每次查询全新 DuckDB 只读连接（只挂 httpfs 不挂 MySQL）、仅允许单条 SELECT/WITH、强制超时与行数上限。

**时间处理**（`timeutil.py`）：时间列支持 `unix_s/unix_ms/unix_us/datetime/timestamp` 五种类型，两套转换——MySQL 侧 WHERE 谓词（须可走索引）与 DuckDB 侧 dt 分区日期推导。改动时注意各类型的时区语义不同（datetime 按 job 时区解释墙钟，timestamp 按 UTC 瞬间）。

**命名约定**（`naming.py`）：文件名解析/生成的唯一出处。`tmp-` 前缀 + `.tmp` 后缀的中间产物既不参与水位解析，也不被查询通配符 `*/*.parquet` 命中。

## 约定

- 代码注释与日志均为中文；结构化 JSON 日志走 stdout（`logs.py`）；
- 配置：单一 YAML（pydantic 校验，`config.py`），默认 `/etc/ebb/config.yml`，`EBB_CONFIG` 或 `-c` 覆盖；样例见 `config.yml.example`；
- 新表接入标准流程：`check` → `status` → `run --once --dry-run` → `run --once` → 加入 `serve`。
