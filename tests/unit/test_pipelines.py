import asyncio
from datetime import UTC, datetime

import mongomock
import pytest
from scrapy.exceptions import DropItem
from scrapy.utils.test import get_crawler

from tests.conftest import AsyncClientShim, AsyncCollectionShim
from wrc_pipeline.config import Settings
from wrc_pipeline.constants import Body
from wrc_pipeline.models import DocKind
from wrc_pipeline.scraping.items import AttachmentItem, DocumentItem
from wrc_pipeline.scraping.pipelines import HashingPipeline, PersistencePipeline
from wrc_pipeline.scraping.spiders.decisions import DecisionsSpider
from wrc_pipeline.storage.mongo import AsyncMetadataRepository, AsyncRunReportStore
from wrc_pipeline.storage.s3 import ObjectStore

HTML_V1 = b'<html><body><div class="content">Decision text</div></body></html><!-- Elapsed time: 0.015 -->'
HTML_V1_REFETCH = (
    b'<html><body><div class="content">Decision text</div></body></html><!-- Elapsed time: 0 -->'
)
HTML_V2 = b'<html><body><div class="content">Amended decision text</div></body></html><!-- Elapsed time: 2 -->'


def make_document_item(raw: bytes = HTML_V1, identifier: str = "ADJ-00053864") -> DocumentItem:
    return DocumentItem(
        identifier=identifier,
        title=identifier,
        description="Sandra Olszewska v Ospg Ennis",
        published_date=datetime(2025, 6, 10, tzinfo=UTC),
        partition_key="2025-06",
        partition_date=datetime(2025, 6, 1, tzinfo=UTC),
        body=Body.WORKPLACE_RELATIONS_COMMISSION,
        source_page_url="https://www.workplacerelations.ie/en/search/?pageNumber=1",
        doc_url="https://www.workplacerelations.ie/en/cases/2025/june/adj-00053864.html",
        doc_kind=DocKind.HTML_PAGE,
        raw_body=raw,
        content_type_header="text/html; charset=utf-8",
    )


def make_attachment_item(identifier: str = "EE47-1999") -> AttachmentItem:
    return AttachmentItem(
        identifier=identifier,
        body=Body.EQUALITY_TRIBUNAL,
        partition_key="1999-12",
        url="https://www.workplacerelations.ie/en/Equality_Tribunal_Import/EE-1999-47.pdf",
        index=1,
        raw_body=b"%PDF-1.4 legacy decision",
        content_type_header="application/pdf",
    )


@pytest.fixture
def spider() -> DecisionsSpider:
    crawler = get_crawler(DecisionsSpider)
    spider = DecisionsSpider.from_crawler(crawler, start_date="2025-06-01", end_date="2025-06-30")
    crawler.spider = spider  # what Crawler.crawl() does before pipelines open
    return spider


class TestHashingPipeline:
    def test_html_document_fields(self, spider: DecisionsSpider) -> None:
        item = HashingPipeline().process_item(make_document_item())

        assert item.file_extension == ".html"
        assert item.content_type == "text/html; charset=utf-8"
        assert item.file_size == len(HTML_V1)
        assert len(item.file_hash) == 64
        assert item.file_path == "landing/body=15376/partition=2025-06/ADJ-00053864.html"

    def test_volatile_comment_changes_file_hash_but_not_content_hash(
        self, spider: DecisionsSpider
    ) -> None:
        pipeline = HashingPipeline()
        first = pipeline.process_item(make_document_item(HTML_V1))
        refetch = pipeline.process_item(make_document_item(HTML_V1_REFETCH))

        assert first.file_hash != refetch.file_hash
        assert first.content_hash == refetch.content_hash

    def test_pdf_attachment_fields(self, spider: DecisionsSpider) -> None:
        item = HashingPipeline().process_item(make_attachment_item())

        assert item.file_extension == ".pdf"
        assert item.content_type == "application/pdf"
        assert item.content_hash == item.file_hash  # binary: no canonicalization
        assert item.file_path == "landing/body=1/partition=1999-12/EE47-1999__attachment_1.pdf"


@pytest.fixture
def persistence(settings: Settings, s3_client, spider: DecisionsSpider) -> PersistencePipeline:
    """PersistencePipeline wired to mongomock (behind an async shim) + moto."""
    pipeline = PersistencePipeline(settings, spider.crawler)
    client: mongomock.MongoClient = mongomock.MongoClient(tz_aware=True)
    database = client[settings.mongo.database]
    pipeline._client = AsyncClientShim()  # type: ignore[assignment]
    pipeline._landing = AsyncMetadataRepository(
        AsyncCollectionShim(database[settings.mongo.landing_collection])  # type: ignore[arg-type]
    )
    pipeline._runs = AsyncRunReportStore(
        AsyncCollectionShim(database[settings.mongo.runs_collection])  # type: ignore[arg-type]
    )
    pipeline._store = ObjectStore(s3_client)
    pipeline._bucket = settings.object_store.landing_bucket
    pipeline._store.ensure_bucket(pipeline._bucket)
    pipeline._mongo_database = database  # test hook
    return pipeline


def stored_keys(s3_client, bucket: str) -> list[str]:
    listing = s3_client.list_objects_v2(Bucket=bucket)
    return [entry["Key"] for entry in listing.get("Contents", [])]


class TestPersistencePipelineIdempotency:
    def process(self, persistence: PersistencePipeline, item) -> None:
        """One item through both pipelines; the persistence step is a coroutine."""
        hashed = HashingPipeline().process_item(item)
        asyncio.run(persistence.process_item(hashed))

    def test_first_run_uploads_and_upserts(
        self,
        persistence: PersistencePipeline,
        spider: DecisionsSpider,
        settings: Settings,
        s3_client,
    ) -> None:
        self.process(persistence, make_document_item())

        assert stored_keys(s3_client, settings.object_store.landing_bucket) == [
            "landing/body=15376/partition=2025-06/ADJ-00053864.html"
        ]
        record = persistence._mongo_database[settings.mongo.landing_collection].find_one(
            {"_id": "ADJ-00053864"}
        )
        assert record is not None
        assert record["file_path"] == "landing/body=15376/partition=2025-06/ADJ-00053864.html"
        assert record["partition_key"] == "2025-06"
        stats = spider.crawler.stats
        assert stats.get_value("wrc/files_uploaded") == 1
        assert stats.get_value("wrc/records_inserted") == 1
        assert stats.get_value("wrc/records_scraped") == 1

    def test_rerun_with_unchanged_content_skips_upload(
        self,
        persistence: PersistencePipeline,
        spider: DecisionsSpider,
        settings: Settings,
        s3_client,
    ) -> None:
        """Idempotency: the refetch differs only by the volatile server comment,
        so no new upload happens and the record is only touched."""
        self.process(persistence, make_document_item(HTML_V1))
        first = persistence._mongo_database[settings.mongo.landing_collection].find_one(
            {"_id": "ADJ-00053864"}
        )

        self.process(persistence, make_document_item(HTML_V1_REFETCH))

        stats = spider.crawler.stats
        assert stats.get_value("wrc/files_uploaded") == 1
        assert stats.get_value("wrc/files_skipped_unchanged") == 1
        assert stats.get_value("wrc/records_scraped") == 2
        second = persistence._mongo_database[settings.mongo.landing_collection].find_one(
            {"_id": "ADJ-00053864"}
        )
        assert first is not None
        assert second is not None
        assert second["file_hash"] == first["file_hash"]  # stored bytes untouched
        assert (
            persistence._mongo_database[settings.mongo.landing_collection].count_documents({}) == 1
        )

    def test_changed_content_is_reuploaded(
        self, persistence: PersistencePipeline, spider: DecisionsSpider, settings: Settings
    ) -> None:
        self.process(persistence, make_document_item(HTML_V1))
        self.process(persistence, make_document_item(HTML_V2))

        stats = spider.crawler.stats
        assert stats.get_value("wrc/files_uploaded") == 2
        assert stats.get_value("wrc/files_skipped_unchanged") is None
        record = persistence._mongo_database[settings.mongo.landing_collection].find_one(
            {"_id": "ADJ-00053864"}
        )
        assert record is not None
        collection = persistence._mongo_database[settings.mongo.landing_collection]
        assert collection.count_documents({}) == 1

    def test_attachment_persisted_and_linked(
        self,
        persistence: PersistencePipeline,
        spider: DecisionsSpider,
        settings: Settings,
        s3_client,
    ) -> None:
        self.process(persistence, make_attachment_item())

        assert stored_keys(s3_client, settings.object_store.landing_bucket) == [
            "landing/body=1/partition=1999-12/EE47-1999__attachment_1.pdf"
        ]
        record = persistence._mongo_database[settings.mongo.landing_collection].find_one(
            {"_id": "EE47-1999"}
        )
        assert record is not None
        assert len(record["attachments"]) == 1
        assert record["attachments"][0]["content_type"] == "application/pdf"


class TestPersistenceConcurrency:
    def test_items_persist_concurrently_on_one_event_loop(
        self, persistence: PersistencePipeline, spider: DecisionsSpider, settings: Settings
    ) -> None:
        """Scrapy awaits process_item for many items at once (CONCURRENT_ITEMS);
        interleaved coroutines must each land their own record."""
        hashing = HashingPipeline()
        items = [
            hashing.process_item(make_document_item(identifier=f"ADJ-{n:08d}")) for n in range(5)
        ]

        async def run_all() -> None:
            await asyncio.gather(*(persistence.process_item(item) for item in items))

        asyncio.run(run_all())

        shim = persistence._landing._collection  # the AsyncCollectionShim
        assert shim.max_in_flight > 1, "items ran one after another instead of overlapping"

        collection = persistence._mongo_database[settings.mongo.landing_collection]
        assert collection.count_documents({}) == 5
        assert spider.crawler.stats.get_value("wrc/records_scraped") == 5

    def test_storage_error_becomes_logged_failure_and_drop(
        self, persistence: PersistencePipeline, spider: DecisionsSpider
    ) -> None:
        async def boom(record):
            raise RuntimeError("mongo down")

        persistence._landing.upsert_record = boom  # type: ignore[method-assign]
        item = HashingPipeline().process_item(make_document_item())

        with pytest.raises(DropItem):
            asyncio.run(persistence.process_item(item))

        assert spider.failures[0]["reason"] == "persistence_error"
        assert spider.failures[0]["identifier"] == "ADJ-00053864"


class TestRunReport:
    def test_report_written_on_spider_close(
        self, persistence: PersistencePipeline, spider: DecisionsSpider, settings: Settings
    ) -> None:
        hashed = HashingPipeline().process_item(make_document_item())
        asyncio.run(persistence.process_item(hashed))
        spider.crawler.stats.inc_value("wrc/records_found", 192)

        asyncio.run(persistence._on_spider_closed(spider, reason="finished"))

        report = persistence._mongo_database[settings.mongo.runs_collection].find_one(
            {"run_id": spider.run_id}
        )
        assert report is not None
        assert report["finish_reason"] == "finished"
        assert report["records_found"] == 192
        assert report["records_scraped"] == 1
        assert report["partitions"] == ["2025-06"]
