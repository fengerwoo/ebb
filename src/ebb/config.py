"""配置加载与校验。

单一 YAML 文件描述 sources（MySQL）、storages（S3 兼容对象存储）、jobs（表级管道）
与可选的 query_api。默认路径 /etc/ebb/config.yml，可用环境变量 EBB_CONFIG 或
CLI 参数 -c 覆盖。
"""

from __future__ import annotations

import os
import re
from typing import Literal
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

DEFAULT_CONFIG_PATH = "/etc/ebb/config.yml"

TimeColumnType = Literal["unix_s", "unix_ms", "unix_us", "datetime", "timestamp"]


def _validate_listen(v: str) -> str:
    """listen 必须是 host:port 形式，配置加载时就报错，而不是启动时 int() 崩溃。"""
    _, sep, port = v.rpartition(":")
    if not sep or not port.isdigit() or not 0 <= int(port) <= 65535:
        raise ValueError(f"listen 必须是 host:port 形式（端口 0-65535）: {v!r}")
    return v


class SourceConfig(BaseModel):
    """一个 MySQL 数据源，DSN 形如 mysql://user:pass@host:3306/dbname。"""

    dsn: str

    @field_validator("dsn")
    @classmethod
    def _check_dsn(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme != "mysql":
            raise ValueError("dsn 必须以 mysql:// 开头")
        if not parsed.hostname or not parsed.path.lstrip("/"):
            raise ValueError("dsn 缺少主机或数据库名")
        return v

    @property
    def parts(self) -> dict:
        p = urlparse(self.dsn)
        return {
            "host": p.hostname,
            "port": p.port or 3306,
            "user": unquote(p.username or "root"),
            "password": unquote(p.password or ""),
            "database": p.path.lstrip("/"),
        }


class StorageConfig(BaseModel):
    """S3 兼容对象存储（AWS S3 / 阿里云 OSS / 腾讯云 COS / MinIO 等）。"""

    type: Literal["s3"] = "s3"
    endpoint: str = ""  # 留空表示 AWS S3 官方端点
    bucket: str
    access_key_id: str
    secret_access_key: str
    region: str = ""
    url_style: Literal["vhost", "path"] = "vhost"  # OSS/COS 用 vhost，MinIO 用 path
    use_ssl: bool = True

    @property
    def endpoint_url(self) -> str | None:
        """boto3 所需的完整 endpoint URL；AWS 官方端点返回 None。"""
        if not self.endpoint:
            return None
        if re.match(r"^https?://", self.endpoint):
            return self.endpoint
        scheme = "https" if self.use_ssl else "http"
        return f"{scheme}://{self.endpoint}"

    @property
    def duckdb_endpoint(self) -> str:
        """DuckDB S3 secret 的 ENDPOINT 字段（不带 scheme）。"""
        return re.sub(r"^https?://", "", self.endpoint)


class ScheduleConfig(BaseModel):
    enabled: bool = True  # 是否参与 serve 调度；关闭后仅可手动执行（便于测试）
    interval_seconds: int = Field(default=300, ge=1)  # 增量导出周期
    compact_at: str = "03:00"  # 每日合并时刻（job 时区），合并后顺带 purge
    # 设置后 purge 按该间隔独立调度（保留期短于一天时必配，否则每天只删一次）；
    # 此时每日任务只做合并，不再顺带 purge。
    purge_interval_seconds: int | None = Field(default=None, ge=1)

    @field_validator("compact_at")
    @classmethod
    def _check_hhmm(cls, v: str) -> str:
        if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", v):
            raise ValueError("compact_at 必须是 HH:MM 格式")
        return v

    @property
    def compact_hour_minute(self) -> tuple[int, int]:
        h, m = self.compact_at.split(":")
        return int(h), int(m)


class BatchConfig(BaseModel):
    export_rows: int = Field(default=500_000, ge=1)  # 单轮导出行数上限
    delete_rows: int = Field(default=5_000, ge=1)  # 删除批大小
    delete_sleep_ms: int = Field(default=200, ge=0)  # 删除批间隔
    # 只导出「时间早于 now - safety_lag_seconds」的行，规避自增 id 提交乱序的
    # 可见性竞态（id 较小的事务后提交，导致按 id 游标漏数据）。
    # 注意它只在「时间列 ≈ 提交时间且事务时长 < lag」的假设下成立，
    # 机制级的保证由 trx_guard 提供；它同时充当 trx_guard 的观察窗口时长。
    safety_lag_seconds: int = Field(default=5, ge=0)
    # 活跃事务守卫：用「自增计数器 + 活跃写事务观察窗口」推导本轮安全 id 上界，
    # 机制上杜绝「小 id 事务晚提交被水位跳过」漏归档（含旧时间值晚提交这种
    # safety_lag 挡不住的情况）。检测到跨窗口写事务时本轮停写、下轮重试。
    # 需要 PROCESS 权限，游标列必须是 AUTO_INCREMENT；ebb check 会校验。
    trx_guard: bool = True


class RetentionConfig(BaseModel):
    online_retain_seconds: int = Field(default=86_400, ge=0)  # 线上保留时长
    verify_before_delete: bool = True  # 删除前校验对象存储侧行数


class JobConfig(BaseModel):
    name: str
    source: str
    table: str
    cursor_column: str = "id"
    time_column: str
    time_column_type: TimeColumnType = "datetime"
    timezone: str = "UTC"
    storage: str
    prefix: str  # 对象存储内的根前缀，分区目录直接挂在其下
    schedule: ScheduleConfig = ScheduleConfig()
    batch: BatchConfig = BatchConfig()
    retention: RetentionConfig = RetentionConfig()

    @field_validator("name")
    @classmethod
    def _check_name(cls, v: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", v):
            raise ValueError("job 名只允许字母、数字、下划线、连字符")
        return v

    @field_validator("table", "cursor_column", "time_column")
    @classmethod
    def _check_identifier(cls, v: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_$]+", v):
            raise ValueError(f"非法标识符: {v!r}")
        return v

    @field_validator("timezone")
    @classmethod
    def _check_tz(cls, v: str) -> str:
        ZoneInfo(v)  # 不存在会抛 KeyError/ZoneInfoNotFoundError
        return v

    @field_validator("prefix")
    @classmethod
    def _check_prefix(cls, v: str) -> str:
        v = v.strip("/")
        if not v:
            raise ValueError("prefix 不能为空")
        return v

    @property
    def tzinfo(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


class QueryApiConfig(BaseModel):
    enabled: bool = False
    listen: str = "0.0.0.0:18761"
    api_keys: list[str] = []
    max_rows: int = Field(default=100_000, ge=1)
    timeout_seconds: int = Field(default=60, ge=1)

    _check_listen = field_validator("listen")(_validate_listen)

    @property
    def host_port(self) -> tuple[str, int]:
        host, _, port = self.listen.rpartition(":")
        return host or "0.0.0.0", int(port)

    @model_validator(mode="after")
    def _check_keys(self) -> "QueryApiConfig":
        if self.enabled and not self.api_keys:
            raise ValueError("query_api.enabled 时 api_keys 不能为空")
        return self


class AdminConfig(BaseModel):
    """serve 进程的本地管理端点（ebb ps 读取），只监听回环地址。"""

    listen: str = "127.0.0.1:18762"

    _check_listen = field_validator("listen")(_validate_listen)

    @property
    def host_port(self) -> tuple[str, int]:
        host, _, port = self.listen.rpartition(":")
        return host or "127.0.0.1", int(port)


class Config(BaseModel):
    sources: dict[str, SourceConfig]
    storages: dict[str, StorageConfig]
    jobs: list[JobConfig]
    query_api: QueryApiConfig = QueryApiConfig()
    admin: AdminConfig = AdminConfig()

    @model_validator(mode="after")
    def _check_refs(self) -> "Config":
        names = set()
        # 水位完全由 prefix 下的文件名重建，两个 job 共用同一个 bucket+prefix
        # 会互相抬高对方水位、静默跳过未归档数据，必须在配置层拒绝。
        # 按 endpoint+bucket 判重（而不是 storage 名）：不同 storage 条目
        # 指向同一个 bucket 时同样冲突。
        prefix_owner: dict[tuple[str, str, str], str] = {}
        for job in self.jobs:
            if job.name in names:
                raise ValueError(f"job 名重复: {job.name}")
            names.add(job.name)
            if job.source not in self.sources:
                raise ValueError(f"job {job.name} 引用了不存在的 source: {job.source}")
            if job.storage not in self.storages:
                raise ValueError(f"job {job.name} 引用了不存在的 storage: {job.storage}")
            st = self.storages[job.storage]
            key = (st.endpoint, st.bucket, job.prefix)
            if key in prefix_owner:
                raise ValueError(
                    f"job {job.name} 与 {prefix_owner[key]} 在同一 bucket "
                    f"({st.bucket}) 使用了相同 prefix: {job.prefix}（水位会互相污染）"
                )
            prefix_owner[key] = job.name
        return self

    def job(self, name: str) -> JobConfig:
        for j in self.jobs:
            if j.name == name:
                return j
        raise KeyError(f"job not found: {name}")

    def source_of(self, job: JobConfig) -> SourceConfig:
        return self.sources[job.source]

    def storage_of(self, job: JobConfig) -> StorageConfig:
        return self.storages[job.storage]


def resolve_config_path(cli_path: str | None = None) -> str:
    return cli_path or os.environ.get("EBB_CONFIG") or DEFAULT_CONFIG_PATH


def load_config(path: str | None = None) -> Config:
    real = resolve_config_path(path)
    with open(real, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"配置文件格式错误: {real}")
    return Config.model_validate(data)
