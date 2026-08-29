"""Domain models shared by ingestion and transformation.

Pydantic models validate records at the system boundary (before anything is
persisted); storage layers receive plain dicts produced by `.to_document()`.
"""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator

from wrc_pipeline.constants import BODY_LABELS, Body


class DocKind(StrEnum):
    HTML_PAGE = "html_page"  # "View Page" led to an HTML case page (spec 6b)
    FILE = "file"  # the record linked a PDF/DOC directly (spec 6a)


class AttachmentRef(BaseModel):
    """A PDF/DOC linked from inside a case page (legacy decisions)."""

    url: str
    file_path: str
    file_hash: str
    content_type: str
    file_size: int


class DecisionRecord(BaseModel):
    """One decision/determination as stored in the landing collection."""

    identifier: str = Field(min_length=1)
    title: str
    description: str | None = None
    published_date: datetime | None = None
    partition_date: datetime
    partition_key: str  # "YYYY-MM", convenient for humans and object keys
    body: Body
    body_label: str = ""
    source_page_url: str  # the search results page the record was found on
    doc_url: str  # the "View Page" target (case page or direct file)
    doc_kind: DocKind

    file_path: str  # object key in the landing bucket
    file_hash: str  # sha256 of the exact stored bytes
    content_hash: str  # sha256 of canonicalized bytes; drives change detection
    content_type: str
    file_size: int
    file_extension: str

    scraped_at: datetime
    run_id: str

    @model_validator(mode="after")
    def _fill_body_label(self) -> Self:
        if not self.body_label:
            self.body_label = BODY_LABELS[self.body]
        return self

    def to_document(self) -> dict[str, Any]:
        doc = self.model_dump()
        doc["body"] = int(self.body)
        return doc


class RunReport(BaseModel):
    """Summary of one ingestion run, persisted for auditing and asset checks."""

    run_id: str
    spider: str
    start_date: str
    end_date: str
    bodies: list[int]
    partitions: list[str]
    records_found: int = 0
    records_scraped: int = 0
    files_uploaded: int = 0
    files_skipped_unchanged: int = 0
    attachments_downloaded: int = 0
    failures: list[dict[str, Any]] = Field(default_factory=list)
    started_at: datetime
    finished_at: datetime | None = None
    finish_reason: str | None = None


def utcnow() -> datetime:
    return datetime.now(tz=UTC)
