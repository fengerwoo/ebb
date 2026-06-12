"""存储布局与文件名约定。

所有数据文件名都携带 id 区间，水位可以纯靠列举文件名重建：

    {prefix}/dt=YYYY-MM-DD/inc-{from_id}-{to_id}.parquet    增量小文件
    {prefix}/dt=YYYY-MM-DD/data-{from_id}-{to_id}.parquet   天级合并文件

合并过程的中间产物用 .tmp 后缀（tmp-data-{from_id}-{to_id}.tmp），既不被
水位解析匹配，也不被查询通配符（*.parquet）命中。
"""

from __future__ import annotations

import re
from dataclasses import dataclass

FILE_RE = re.compile(r"^(?P<kind>inc|data)-(?P<from_id>\d+)-(?P<to_id>\d+)\.parquet$")
DT_RE = re.compile(r"^dt=(?P<dt>\d{4}-\d{2}-\d{2})$")


@dataclass(frozen=True)
class DataFile:
    """一个已归档的数据文件（从对象 key 解析而来）。"""

    key: str  # 完整对象 key
    dt: str  # 分区日期 YYYY-MM-DD
    kind: str  # inc / data
    from_id: int
    to_id: int


def partition_dir(prefix: str, dt: str) -> str:
    return f"{prefix}/dt={dt}"


def inc_key(prefix: str, dt: str, from_id: int, to_id: int) -> str:
    return f"{partition_dir(prefix, dt)}/inc-{from_id}-{to_id}.parquet"


def data_key(prefix: str, dt: str, from_id: int, to_id: int) -> str:
    return f"{partition_dir(prefix, dt)}/data-{from_id}-{to_id}.parquet"


def tmp_key(prefix: str, dt: str, from_id: int, to_id: int) -> str:
    return f"{partition_dir(prefix, dt)}/tmp-data-{from_id}-{to_id}.tmp"


def parse_key(prefix: str, key: str) -> DataFile | None:
    """解析对象 key，不符合数据文件命名约定的返回 None。"""
    if not key.startswith(prefix + "/"):
        return None
    rest = key[len(prefix) + 1 :]
    parts = rest.split("/")
    if len(parts) != 2:
        return None
    m_dt = DT_RE.match(parts[0])
    m_file = FILE_RE.match(parts[1])
    if not m_dt or not m_file:
        return None
    from_id = int(m_file["from_id"])
    to_id = int(m_file["to_id"])
    if from_id > to_id:
        return None
    return DataFile(
        key=key,
        dt=m_dt["dt"],
        kind=m_file["kind"],
        from_id=from_id,
        to_id=to_id,
    )


def parse_keys(prefix: str, keys: list[str]) -> list[DataFile]:
    return [f for f in (parse_key(prefix, k) for k in keys) if f is not None]


def watermark_of(files: list[DataFile]) -> int:
    """水位 = 所有数据文件 to_id 的最大值；没有文件时为 0。"""
    return max((f.to_id for f in files), default=0)
