"""S3-compatible object store adapter (MinIO locally, portable to real S3).

boto3 with an explicit ``endpoint_url`` is the only MinIO-specific piece —
moving to AWS S3 means changing one environment variable. Clients are
thread-safe and created once per process.
"""

from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from wrc_pipeline.config import ObjectStoreSettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
else:  # pragma: no cover - typing convenience only
    S3Client = Any


def create_s3_client(settings: ObjectStoreSettings) -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key.get_secret_value(),
        aws_secret_access_key=settings.secret_key.get_secret_value(),
        region_name=settings.region,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},  # virtual-host style needs per-bucket DNS
            retries={"max_attempts": 5, "mode": "standard"},
            request_checksum_calculation=settings.request_checksum_calculation,
            response_checksum_validation=settings.response_checksum_validation,
        ),
    )


class ObjectStore:
    def __init__(self, client: S3Client) -> None:
        self._client = client

    def ensure_bucket(self, bucket: str) -> None:
        try:
            self._client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code not in {"404", "NoSuchBucket"}:
                raise
            self._client.create_bucket(Bucket=bucket)

    def put_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str,
        metadata: dict[str, str] | None = None,
    ) -> None:
        # Metadata values must be ASCII-safe strings — hex digests, ISO
        # timestamps and URLs only; never titles (Irish fadas would mangle).
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            Metadata=metadata or {},
        )

    def get_bytes(self, bucket: str, key: str) -> bytes:
        response = self._client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def object_exists(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in {"404", "NoSuchKey"}:
                return False
            raise
        return True
