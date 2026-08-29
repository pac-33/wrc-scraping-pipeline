"""Item pipelines: derive file fields, then persist with change detection.

Order matters:

1. ``HashingPipeline`` — pure derivation (hashes, extension, content type,
   object key). No I/O, trivially testable.
2. ``PersistencePipeline`` — the single place that talks to storage. Per
   document: compare the canonical content hash against MongoDB; unchanged
   documents only get their seen-markers touched (no upload), changed/new
   documents are uploaded first and upserted after — so a landing record
   always points at bytes that exist in the bucket.
"""

from typing import Any

from itemadapter import ItemAdapter
from scrapy import Spider, signals
from scrapy.crawler import Crawler
from scrapy.exceptions import DropItem

from wrc_pipeline.config import Settings, get_settings
from wrc_pipeline.constants import EXTENSION_BY_CONTENT_TYPE
from wrc_pipeline.hashing import content_hash, file_hash
from wrc_pipeline.models import AttachmentRef, DecisionRecord, RunReport, utcnow
from wrc_pipeline.naming import attachment_key, detect_extension, landing_key
from wrc_pipeline.scraping.items import AttachmentItem, DocumentItem
from wrc_pipeline.storage.mongo import (
    MetadataRepository,
    RunReportStore,
    create_mongo_client,
    ensure_indexes,
    get_database,
)
from wrc_pipeline.storage.s3 import ObjectStore, create_s3_client


class HashingPipeline:
    """Fill the derived file fields on every item."""

    def process_item(self, item: DocumentItem | AttachmentItem, spider: Spider) -> Any:
        adapter = ItemAdapter(item)
        raw: bytes = adapter["raw_body"]
        source_url = item.url if isinstance(item, AttachmentItem) else item.doc_url
        extension = detect_extension(source_url, adapter.get("content_type_header"), raw)
        is_html = extension == ".html"

        adapter["file_extension"] = extension
        adapter["file_hash"] = file_hash(raw)
        adapter["content_hash"] = content_hash(raw, is_html=is_html)
        adapter["file_size"] = len(raw)
        adapter["content_type"] = _final_content_type(adapter.get("content_type_header"), extension)
        if isinstance(item, AttachmentItem):
            adapter["file_path"] = attachment_key(
                item.body, item.partition_key, item.identifier, item.index, extension
            )
        else:
            adapter["file_path"] = landing_key(
                item.body, item.partition_key, item.identifier, extension
            )
        return item


class PersistencePipeline:
    """Change-detect against MongoDB, upload to the landing bucket, upsert
    metadata, and write the end-of-run report."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "PersistencePipeline":
        return cls(get_settings())

    def open_spider(self, spider: Spider) -> None:
        # spider_closed (unlike pipeline close_spider) delivers the close
        # reason, which belongs in the run report.
        spider.crawler.signals.connect(self._on_spider_closed, signal=signals.spider_closed)
        self._client = create_mongo_client(self._settings.mongo)
        database = get_database(self._client, self._settings.mongo)
        ensure_indexes(database, self._settings.mongo)
        self._landing = MetadataRepository(database[self._settings.mongo.landing_collection])
        self._runs = RunReportStore(database[self._settings.mongo.runs_collection])
        self._store = ObjectStore(create_s3_client(self._settings.object_store))
        self._bucket = self._settings.object_store.landing_bucket
        self._store.ensure_bucket(self._bucket)
        self._store.ensure_bucket(self._settings.object_store.curated_bucket)

    def process_item(self, item: DocumentItem | AttachmentItem, spider: Spider) -> Any:
        try:
            if isinstance(item, AttachmentItem):
                return self._persist_attachment(item, spider)
            return self._persist_document(item, spider)
        except DropItem:
            raise
        except Exception as exc:
            self._record_failure(
                spider,
                reason="persistence_error",
                identifier=ItemAdapter(item).get("identifier"),
                detail=repr(exc),
            )
            msg = f"persistence failed for {ItemAdapter(item).get('identifier')}: {exc!r}"
            raise DropItem(msg) from exc

    def _persist_document(self, item: DocumentItem, spider: Spider) -> DocumentItem:
        now = utcnow()
        stored_hash = self._landing.get_content_hash(item.identifier)
        run_id: str = getattr(spider, "run_id", "unknown")

        if stored_hash == item.content_hash:
            self._landing.touch_unchanged(item.identifier, run_id, now)
            spider.crawler.stats.inc_value("wrc/files_skipped_unchanged")
            spider.logger.debug(
                "document_unchanged",
                extra={"identifier": item.identifier, "partition": item.partition_key},
            )
        else:
            self._store.put_bytes(
                self._bucket,
                item.file_path,
                item.raw_body,
                content_type=item.content_type,
                metadata={
                    "sha256": item.file_hash,
                    "source-url": item.doc_url,
                    "scraped-at": now.isoformat(),
                },
            )
            spider.crawler.stats.inc_value("wrc/files_uploaded")
            record = DecisionRecord(
                identifier=item.identifier,
                title=item.title,
                description=item.description,
                published_date=item.published_date,
                partition_date=item.partition_date,
                partition_key=item.partition_key,
                body=item.body,
                source_page_url=item.source_page_url,
                doc_url=item.doc_url,
                doc_kind=item.doc_kind,
                file_path=item.file_path,
                file_hash=item.file_hash,
                content_hash=item.content_hash,
                content_type=item.content_type,
                file_size=item.file_size,
                file_extension=item.file_extension,
                scraped_at=now,
                run_id=run_id,
            )
            inserted = self._landing.upsert_record(record)
            spider.crawler.stats.inc_value(
                "wrc/records_inserted" if inserted else "wrc/records_updated"
            )
            spider.logger.info(
                "document_stored",
                extra={
                    "identifier": item.identifier,
                    "partition": item.partition_key,
                    "file_path": item.file_path,
                    "file_size": item.file_size,
                    "changed": stored_hash is not None,
                },
            )
        spider.crawler.stats.inc_value("wrc/records_scraped")
        return item

    def _persist_attachment(self, item: AttachmentItem, spider: Spider) -> AttachmentItem:
        self._store.put_bytes(
            self._bucket,
            item.file_path,
            item.raw_body,
            content_type=item.content_type,
            metadata={"sha256": item.file_hash, "source-url": item.url},
        )
        self._landing.add_attachment(
            item.identifier,
            AttachmentRef(
                url=item.url,
                file_path=item.file_path,
                file_hash=item.file_hash,
                content_type=item.content_type,
                file_size=item.file_size,
            ),
        )
        spider.crawler.stats.inc_value("wrc/attachments_downloaded")
        return item

    def _on_spider_closed(self, spider: Spider, reason: str) -> None:
        try:
            self._write_run_report(spider, reason)
        finally:
            self._client.close()

    def _write_run_report(self, spider: Spider, reason: str) -> None:
        stats = spider.crawler.stats
        report = RunReport(
            run_id=getattr(spider, "run_id", "unknown"),
            spider=spider.name,
            start_date=getattr(spider, "start_date_arg", ""),
            end_date=getattr(spider, "end_date_arg", ""),
            bodies=[int(b) for b in getattr(spider, "bodies", [])],
            partitions=[p.key for p in getattr(spider, "partitions", [])],
            records_found=stats.get_value("wrc/records_found", 0),
            records_scraped=stats.get_value("wrc/records_scraped", 0),
            files_uploaded=stats.get_value("wrc/files_uploaded", 0),
            files_skipped_unchanged=stats.get_value("wrc/files_skipped_unchanged", 0),
            attachments_downloaded=stats.get_value("wrc/attachments_downloaded", 0),
            failures=list(getattr(spider, "failures", [])),
            started_at=getattr(spider, "started_at", utcnow()),
            finished_at=utcnow(),
            finish_reason=reason,
        )
        self._runs.save(report)
        spider.logger.info(
            "run_summary",
            extra={
                "run_id": report.run_id,
                "records_found": report.records_found,
                "records_scraped": report.records_scraped,
                "files_uploaded": report.files_uploaded,
                "files_skipped_unchanged": report.files_skipped_unchanged,
                "attachments_downloaded": report.attachments_downloaded,
                "failures": len(report.failures),
            },
        )

    def _record_failure(self, spider: Spider, **detail: Any) -> None:
        entry = {key: value for key, value in detail.items() if value is not None}
        getattr(spider, "failures", []).append(entry)
        spider.crawler.stats.inc_value("wrc/failures")
        spider.crawler.stats.inc_value(f"wrc/failures/{entry.get('reason', 'unknown')}")
        spider.logger.error("record_failed", extra=entry)


def _final_content_type(header: str | None, extension: str) -> str:
    if header:
        base = header.split(";")[0].strip().lower()
        if base and base != "application/octet-stream":
            return header
    for content_type, ext in EXTENSION_BY_CONTENT_TYPE.items():
        if ext == extension:
            return content_type
    return "application/octet-stream"
