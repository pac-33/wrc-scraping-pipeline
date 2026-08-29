"""Scrapy items carried through the pipeline chain.

Plain dataclasses (accessed via ItemAdapter in pipelines): the parse callbacks
fill the scraped fields, `HashingPipeline` fills the derived file fields, and
`PersistencePipeline` consumes the completed item.
"""

from dataclasses import dataclass, field
from datetime import datetime

from wrc_pipeline.constants import Body
from wrc_pipeline.models import DocKind


@dataclass
class DocumentItem:
    """One decision record plus the raw bytes of its document."""

    identifier: str
    title: str
    description: str | None
    published_date: datetime | None
    partition_key: str
    partition_date: datetime
    body: Body
    source_page_url: str
    doc_url: str
    doc_kind: DocKind
    raw_body: bytes = field(repr=False)
    content_type_header: str | None = None

    # Derived by HashingPipeline:
    file_hash: str = ""
    content_hash: str = ""
    file_extension: str = ""
    file_path: str = ""
    content_type: str = ""
    file_size: int = 0


@dataclass
class AttachmentItem:
    """A PDF/DOC linked from inside a case page (legacy decisions)."""

    identifier: str
    body: Body
    partition_key: str
    url: str
    index: int
    raw_body: bytes = field(repr=False)
    content_type_header: str | None = None

    file_hash: str = ""
    content_hash: str = ""
    file_extension: str = ""
    file_path: str = ""
    content_type: str = ""
    file_size: int = 0
