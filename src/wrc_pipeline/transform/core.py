"""Transformation core: landing zone -> curated zone for a date range.

For every landing record in [start_date, end_date] (by partition_date):

- PDF/DOC documents are copied as-is (spec: no transformation);
- HTML documents are reduced to their relevant content via BeautifulSoup;
- every file is renamed to ``<identifier>.<ext>`` in the curated bucket;
- a curated metadata record (new file path + new file hash + extraction
  quality fields) is upserted into the curated collection.

The landing zone is never written to. Re-runs are idempotent: a record whose
landing ``content_hash`` already matches the curated ``source_content_hash``
is skipped, so only new or re-scraped documents are re-transformed.

This module is orchestrator-agnostic — the CLI (``python -m
wrc_pipeline.transform``) and the Dagster asset are both thin wrappers around
:func:`transform_range`.
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from pymongo.database import Database

from wrc_pipeline.config import Settings
from wrc_pipeline.constants import Body
from wrc_pipeline.hashing import file_hash
from wrc_pipeline.logging import get_logger
from wrc_pipeline.models import utcnow
from wrc_pipeline.naming import curated_key
from wrc_pipeline.storage.mongo import (
    CuratedRepository,
    MetadataRepository,
    create_mongo_client,
    ensure_indexes,
    get_database,
)
from wrc_pipeline.storage.s3 import ObjectStore, create_s3_client
from wrc_pipeline.transform.extract import extract_relevant_content

logger = get_logger(__name__)

_HTML_EXTENSION = ".html"


@dataclass
class TransformStats:
    run_id: str
    records_selected: int = 0
    records_transformed: int = 0
    records_skipped_unchanged: int = 0
    attachments_copied: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "records_selected": self.records_selected,
            "records_transformed": self.records_transformed,
            "records_skipped_unchanged": self.records_skipped_unchanged,
            "attachments_copied": self.attachments_copied,
            "failures": len(self.failures),
        }


def transform_range(
    start_date: date,
    end_date: date,
    settings: Settings,
    bodies: list[Body] | None = None,
    *,
    database: Database[dict[str, Any]] | None = None,
    s3_client: Any = None,
    max_workers: int = 8,
) -> TransformStats:
    """Transform every landing record whose partition_date falls in the range.

    ``database`` / ``s3_client`` are injectable for tests; by default real
    clients are built from settings.
    """
    if database is None:
        client = create_mongo_client(settings.mongo)
        database = get_database(client, settings.mongo)
    if s3_client is None:
        s3_client = create_s3_client(settings.object_store)

    ensure_indexes(database, settings.mongo)
    landing = MetadataRepository(database[settings.mongo.landing_collection])
    curated = CuratedRepository(database[settings.mongo.curated_collection])
    store = ObjectStore(s3_client)
    store.ensure_bucket(settings.object_store.curated_bucket)

    stats = TransformStats(run_id=uuid4().hex)
    body_ids = [int(b) for b in bodies] if bodies else None
    records = list(landing.iter_partition(_as_utc(start_date), _as_utc(end_date), bodies=body_ids))
    stats.records_selected = len(records)
    logger.info(
        "transform_started",
        run_id=stats.run_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        records_selected=stats.records_selected,
    )

    worker = _RecordTransformer(settings, curated, store, stats.run_id)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for outcome in pool.map(worker.transform_record, records):
            _fold_outcome(stats, outcome)

    logger.info("transform_summary", **stats.as_dict())
    return stats


def _as_utc(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def _fold_outcome(stats: TransformStats, outcome: dict[str, Any]) -> None:
    status = outcome.pop("status")
    if status == "transformed":
        stats.records_transformed += 1
        stats.attachments_copied += outcome.get("attachments_copied", 0)
    elif status == "skipped_unchanged":
        stats.records_skipped_unchanged += 1
    else:
        stats.failures.append(outcome)


class _RecordTransformer:
    """Per-record work unit; instances are shared across worker threads
    (pymongo and boto3 clients are thread-safe)."""

    def __init__(
        self, settings: Settings, curated: CuratedRepository, store: ObjectStore, run_id: str
    ) -> None:
        self._settings = settings
        self._curated = curated
        self._store = store
        self._run_id = run_id

    def transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        identifier = record["_id"]
        try:
            if self._curated.get_source_hash(identifier) == record["content_hash"]:
                logger.debug("record_unchanged", identifier=identifier, run_id=self._run_id)
                return {"status": "skipped_unchanged", "identifier": identifier}
            return self._do_transform(record)
        except Exception as exc:  # noqa: BLE001 - per-record isolation: one bad
            # record must not abort the partition; it is logged and reported.
            failure = {
                "status": "failed",
                "identifier": identifier,
                "reason": type(exc).__name__,
                "detail": str(exc),
            }
            logger.error("record_transform_failed", run_id=self._run_id, **failure)
            return failure

    def _do_transform(self, record: dict[str, Any]) -> dict[str, Any]:
        identifier: str = record["_id"]
        body = Body(record["body"])
        landing_bucket = self._settings.object_store.landing_bucket
        curated_bucket = self._settings.object_store.curated_bucket
        raw = self._store.get_bytes(landing_bucket, record["file_path"])

        extraction_fields: dict[str, Any] = {}
        if record["file_extension"] == _HTML_EXTENSION:
            extraction = extract_relevant_content(raw, identifier)
            output_bytes = extraction.html
            content_type = "text/html; charset=utf-8"
            extraction_fields = {
                "extracted_title": extraction.title,
                "content_char_count": extraction.content_char_count,
                "content_is_empty": extraction.content_is_empty,
                "extraction_used_fallback": extraction.used_fallback,
            }
        else:
            # PDF/DOC documents are stored exactly as landed (spec 1.c.i).
            output_bytes = raw
            content_type = record["content_type"]

        new_key = curated_key(body, record["partition_key"], identifier, record["file_extension"])
        new_hash = file_hash(output_bytes)
        self._store.put_bytes(
            curated_bucket,
            new_key,
            output_bytes,
            content_type=content_type,
            metadata={"sha256": new_hash, "source-sha256": record["file_hash"]},
        )

        attachments, attachments_copied = self._copy_attachments(record, body)

        now = utcnow()
        curated_doc = {
            "title": record.get("title"),
            "description": record.get("description"),
            "published_date": record.get("published_date"),
            "partition_date": record["partition_date"],
            "partition_key": record["partition_key"],
            "body": record["body"],
            "body_label": record.get("body_label"),
            "doc_url": record.get("doc_url"),
            "doc_kind": record.get("doc_kind"),
            "file_path": new_key,
            "file_hash": new_hash,
            "file_size": len(output_bytes),
            "file_extension": record["file_extension"],
            "content_type": content_type,
            "source_file_path": record["file_path"],
            "source_file_hash": record["file_hash"],
            "source_content_hash": record["content_hash"],
            "attachments": attachments,
            "transformed_at": now,
            "transform_run_id": self._run_id,
            **extraction_fields,
        }
        self._curated.upsert(identifier, curated_doc, first_seen_at=now)
        logger.info(
            "record_transformed",
            identifier=identifier,
            run_id=self._run_id,
            file_path=new_key,
            file_size=len(output_bytes),
            attachments_copied=attachments_copied,
        )
        return {
            "status": "transformed",
            "identifier": identifier,
            "attachments_copied": attachments_copied,
        }

    def _copy_attachments(
        self, record: dict[str, Any], body: Body
    ) -> tuple[list[dict[str, Any]], int]:
        """Attachments (legacy decision PDFs) are binary — copied as-is under
        the curated naming scheme."""
        curated_bucket = self._settings.object_store.curated_bucket
        landing_bucket = self._settings.object_store.landing_bucket
        identifier: str = record["_id"]
        copied: list[dict[str, Any]] = []
        for index, attachment in enumerate(record.get("attachments", []), start=1):
            data = self._store.get_bytes(landing_bucket, attachment["file_path"])
            extension = attachment["file_path"].rsplit(".", 1)[-1]
            new_key = curated_key(
                body, record["partition_key"], f"{identifier}__attachment_{index}", f".{extension}"
            )
            self._store.put_bytes(
                curated_bucket,
                new_key,
                data,
                content_type=attachment["content_type"],
                metadata={"sha256": attachment["file_hash"]},
            )
            copied.append({**attachment, "file_path": new_key})
        return copied, len(copied)
