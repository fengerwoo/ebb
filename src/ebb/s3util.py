"""对象存储操作（boto3）：LIST（水位）、HEAD（字节数）、改名、删除。

数据面的读写（Parquet 上传/下载/合并/查询）全部走 DuckDB httpfs，
这里只承担控制面的轻量操作。
"""

from __future__ import annotations

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig

from .config import StorageConfig

# 服务端拷贝的分片策略：单次 CopyObject 有大小上限（AWS 5GB、阿里云 OSS 1GB），
# 超过阈值自动切到 UploadPartCopy 分片拷贝；阈值以下仍是单请求，无额外开销
_COPY_CONFIG = TransferConfig(
    multipart_threshold=512 * 1024 * 1024,
    multipart_chunksize=256 * 1024 * 1024,
)


def make_client(storage: StorageConfig):
    addressing = "virtual" if storage.url_style == "vhost" else "path"
    return boto3.client(
        "s3",
        endpoint_url=storage.endpoint_url,
        aws_access_key_id=storage.access_key_id,
        aws_secret_access_key=storage.secret_access_key,
        region_name=storage.region or None,
        config=BotoConfig(
            s3={"addressing_style": addressing},
            retries={"max_attempts": 3, "mode": "standard"},
            # boto3>=1.36 默认对上传启用 aws-chunked 流式校验和，
            # 阿里云 OSS / 腾讯云 COS 等不支持，改回仅必需时计算
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


class S3Store:
    """绑定到一个 bucket 的轻量封装。"""

    def __init__(self, storage: StorageConfig):
        self.storage = storage
        self.bucket = storage.bucket
        self.client = make_client(storage)

    def list_keys(self, prefix: str) -> list[str]:
        """列出 prefix 下全部对象 key（自动翻页）。"""
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix.rstrip("/") + "/"):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def list_objects(self, prefix: str) -> list[dict]:
        """同 list_keys，但保留 Size / LastModified 元信息。"""
        objs: list[dict] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix.rstrip("/") + "/"):
            objs.extend(page.get("Contents", []))
        return objs

    def head_size(self, key: str) -> int:
        return self.client.head_object(Bucket=self.bucket, Key=key)["ContentLength"]

    def rename(self, src_key: str, dst_key: str) -> None:
        """S3 没有原生 rename：服务端 copy + 删除源。

        用托管 copy 而不是裸 copy_object：合并后的天级文件可能超过单次
        CopyObject 的大小上限，托管接口超阈值自动分片，始终在服务端完成。
        """
        self.client.copy(
            CopySource={"Bucket": self.bucket, "Key": src_key},
            Bucket=self.bucket,
            Key=dst_key,
            Config=_COPY_CONFIG,
        )
        self.client.delete_object(Bucket=self.bucket, Key=src_key)

    def delete_keys(self, keys: list[str]) -> None:
        # 不用批量 DeleteObjects：botocore>=1.36 对其强制附加 CRC32 校验头，
        # 阿里云 OSS 只认 Content-MD5 会报 MissingArgument；控制面删除量很小，逐个删即可
        for key in keys:
            self.client.delete_object(Bucket=self.bucket, Key=key)

    def put_probe(self, key: str, body: bytes = b"ebb") -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body)

    def get_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def delete_key(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)
