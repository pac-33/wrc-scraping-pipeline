"""Typed application settings, loaded from environment variables / .env.

Every tunable in the pipeline (connection strings, bucket names, partition
size, scraping parameters) resolves through this module — nothing is hardcoded
at call sites. Nested fields map to env vars with a double-underscore
delimiter, e.g. `WRC_MONGO__URI`, `WRC_OBJECT_STORE__ENDPOINT_URL`,
`WRC_SCRAPER__TARGET_CONCURRENCY`.
"""

from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from wrc_pipeline.constants import Body


class MongoSettings(BaseModel):
    uri: SecretStr
    database: str = "wrc"
    landing_collection: str = "decisions_landing"
    curated_collection: str = "decisions_curated"
    runs_collection: str = "pipeline_runs"


class ObjectStoreSettings(BaseModel):
    endpoint_url: str = "http://localhost:9000"
    access_key: SecretStr
    secret_key: SecretStr
    region: str = "us-east-1"
    landing_bucket: str = "wrc-landing"
    curated_bucket: str = "wrc-curated"
    # boto3 >= 1.36 adds CRC32 integrity checksums by default; some
    # S3-compatible backends reject them, so keep the escape hatch configurable.
    request_checksum_calculation: Literal["when_supported", "when_required"] = "when_supported"
    response_checksum_validation: Literal["when_supported", "when_required"] = "when_supported"


class ScraperSettings(BaseModel):
    bodies: list[Body] = Field(default_factory=lambda: list(Body))
    partition_size: Literal["monthly"] = "monthly"
    contact_email: str = "dataeng@example.com"

    target_concurrency: float = 2.0
    download_delay_floor_seconds: float = 0.25
    max_concurrent_requests_per_domain: int = 4
    download_timeout_seconds: int = 30

    retry_times: int = 3
    retry_after_cap_seconds: int = 120
    backoff_base_seconds: float = 1.0
    # Consecutive exhausted-retry failures before the run aborts (circuit breaker).
    circuit_breaker_threshold: int = 10

    # Legacy decisions link PDF/DOC files under /en/*_Import/ paths that
    # robots.txt disallows for crawlers. The case pages present them as the
    # canonical decision documents, so we fetch them by default with a
    # documented per-request robots exception; set False for strict mode
    # (records are then logged as skipped with the reason).
    fetch_robots_disallowed_documents: bool = True
    download_attachments: bool = True

    dev_http_cache: bool = False


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="WRC_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"
    mongo: MongoSettings
    object_store: ObjectStoreSettings
    scraper: ScraperSettings = Field(default_factory=ScraperSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load settings once per process; fails fast if required vars are missing."""
    # Required fields (mongo, object_store) are populated from env at runtime.
    return Settings()  # type: ignore[call-arg]
