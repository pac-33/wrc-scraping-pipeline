import asyncio
from datetime import UTC, datetime
from unittest.mock import MagicMock

import mongomock
import pytest
from pymongo.errors import DuplicateKeyError

from tests.conftest import AsyncCollectionShim
from wrc_pipeline.constants import Body
from wrc_pipeline.models import AttachmentRef, DecisionRecord, DocKind
from wrc_pipeline.storage.mongo import AsyncMetadataRepository, MetadataRepository


def make_record(**overrides: object) -> DecisionRecord:
    defaults: dict[str, object] = {
        "identifier": "ADJ-00054658",
        "title": "ADJ-00054658",
        "description": "Declan Holden V Ger Brennan Construction",
        "published_date": datetime(2025, 7, 17, tzinfo=UTC),
        "partition_date": datetime(2025, 7, 1, tzinfo=UTC),
        "partition_key": "2025-07",
        "body": Body.WORKPLACE_RELATIONS_COMMISSION,
        "source_page_url": "https://www.workplacerelations.ie/en/search/?pageNumber=1",
        "doc_url": "https://www.workplacerelations.ie/en/cases/2025/july/adj-00054658.html",
        "doc_kind": DocKind.HTML_PAGE,
        "file_path": "landing/body=15376/partition=2025-07/ADJ-00054658.html",
        "file_hash": "a" * 64,
        "content_type": "text/html; charset=utf-8",
        "file_size": 22056,
        "file_extension": ".html",
        "scraped_at": datetime(2025, 8, 1, 12, 0, tzinfo=UTC),
        "run_id": "run-001",
    }
    defaults.update(overrides)
    return DecisionRecord.model_validate(defaults)


@pytest.fixture
def repo(mongo_database: mongomock.Database) -> MetadataRepository:
    return MetadataRepository(mongo_database["decisions_landing"])


class TestUpsertIdempotency:
    def test_first_upsert_inserts(self, repo: MetadataRepository) -> None:
        assert repo.upsert_record(make_record()) is True

    def test_second_upsert_does_not_duplicate(
        self, repo: MetadataRepository, mongo_database: mongomock.Database
    ) -> None:
        repo.upsert_record(make_record())
        inserted_again = repo.upsert_record(make_record(run_id="run-002"))

        assert inserted_again is False
        assert mongo_database["decisions_landing"].count_documents({}) == 1

    def test_rerun_preserves_first_seen_provenance(
        self, repo: MetadataRepository, mongo_database: mongomock.Database
    ) -> None:
        repo.upsert_record(make_record(run_id="run-001"))
        repo.upsert_record(
            make_record(run_id="run-002", scraped_at=datetime(2025, 8, 2, tzinfo=UTC))
        )

        doc = mongo_database["decisions_landing"].find_one({"_id": "ADJ-00054658"})
        assert doc is not None
        assert doc["first_run_id"] == "run-001"
        assert doc["last_run_id"] == "run-002"
        assert doc["first_seen_at"] < doc["last_seen_at"]

    def test_rerun_refreshes_mutable_fields(
        self, repo: MetadataRepository, mongo_database: mongomock.Database
    ) -> None:
        repo.upsert_record(make_record())
        repo.upsert_record(make_record(file_hash="d" * 64))

        doc = mongo_database["decisions_landing"].find_one({"_id": "ADJ-00054658"})
        assert doc is not None
        assert doc["file_hash"] == "d" * 64

    def test_duplicate_key_race_is_retried_once(self) -> None:
        collection = MagicMock()
        ok_result = MagicMock(upserted_id=None)
        collection.update_one.side_effect = [DuplicateKeyError("E11000"), ok_result]

        repo = MetadataRepository(collection)
        inserted = repo.upsert_record(make_record())

        assert inserted is False
        assert collection.update_one.call_count == 2


@pytest.fixture
def async_repo(mongo_database: mongomock.Database) -> AsyncMetadataRepository:
    return AsyncMetadataRepository(
        AsyncCollectionShim(mongo_database["decisions_landing"])  # type: ignore[arg-type]
    )


class TestAsyncRepository:
    """The coroutine flavour used by the item pipeline shares the update logic."""

    def test_upsert_touch_and_lookup(
        self, async_repo: AsyncMetadataRepository, mongo_database: mongomock.Database
    ) -> None:
        async def scenario() -> tuple[bool, bool, str | None]:
            inserted = await async_repo.upsert_record(make_record())
            again = await async_repo.upsert_record(make_record(run_id="run-002"))
            await async_repo.touch_unchanged(
                "ADJ-00054658", "run-003", datetime(2025, 8, 3, tzinfo=UTC)
            )
            return inserted, again, await async_repo.get_file_hash("ADJ-00054658")

        inserted, again, stored_hash = asyncio.run(scenario())

        assert (inserted, again) == (True, False)
        assert stored_hash == "a" * 64
        doc = mongo_database["decisions_landing"].find_one({"_id": "ADJ-00054658"})
        assert doc is not None
        assert doc["first_run_id"] == "run-001"
        assert doc["last_run_id"] == "run-003"
        assert asyncio.run(async_repo.get_file_hash("missing")) is None

    def test_attachment_landing_first_keeps_provenance(
        self, async_repo: AsyncMetadataRepository, mongo_database: mongomock.Database
    ) -> None:
        """With coroutine persistence an attachment can reach Mongo before its
        parent document; the record it creates must carry the provenance the
        parent's ``$setOnInsert`` would otherwise have written."""
        attachment = AttachmentRef(
            url="https://www.workplacerelations.ie/en/Equality_Tribunal_Import/EE-1999-47.pdf",
            file_path="landing/body=1/partition=1999-12/EE47-1999__attachment_1.pdf",
            file_hash="e" * 64,
            content_type="application/pdf",
            file_size=59300,
        )
        seen = datetime(1999, 12, 15, tzinfo=UTC)

        async def scenario() -> None:
            await async_repo.add_attachment("EE47-1999", attachment, "run-001", seen)
            await async_repo.upsert_record(make_record(identifier="EE47-1999", run_id="run-002"))

        asyncio.run(scenario())

        doc = mongo_database["decisions_landing"].find_one({"_id": "EE47-1999"})
        assert doc is not None
        assert doc["first_run_id"] == "run-001"
        assert doc["first_seen_at"].replace(tzinfo=None) == seen.replace(tzinfo=None)
        assert doc["last_run_id"] == "run-002"
        assert doc["title"] == "ADJ-00054658"
        assert len(doc["attachments"]) == 1

    def test_add_attachment_is_idempotent(
        self, async_repo: AsyncMetadataRepository, mongo_database: mongomock.Database
    ) -> None:
        attachment = AttachmentRef(
            url="https://www.workplacerelations.ie/en/Equality_Tribunal_Import/EE-1999-47.pdf",
            file_path="landing/body=1/partition=1999-12/EE47-1999__attachment_1.pdf",
            file_hash="e" * 64,
            content_type="application/pdf",
            file_size=59300,
        )

        seen = datetime(1999, 12, 15, tzinfo=UTC)

        async def scenario() -> None:
            await async_repo.add_attachment("EE47-1999", attachment, "run-001", seen)
            await async_repo.add_attachment("EE47-1999", attachment, "run-001", seen)

        asyncio.run(scenario())

        doc = mongo_database["decisions_landing"].find_one({"_id": "EE47-1999"})
        assert doc is not None
        assert len(doc["attachments"]) == 1


class TestChangeDetection:
    def test_get_file_hash_roundtrip(self, repo: MetadataRepository) -> None:
        assert repo.get_file_hash("ADJ-00054658") is None
        repo.upsert_record(make_record())
        assert repo.get_file_hash("ADJ-00054658") == "a" * 64

    def test_touch_unchanged_updates_only_seen_markers(
        self, repo: MetadataRepository, mongo_database: mongomock.Database
    ) -> None:
        repo.upsert_record(make_record())
        repo.touch_unchanged("ADJ-00054658", "run-002", datetime(2025, 8, 2, tzinfo=UTC))

        doc = mongo_database["decisions_landing"].find_one({"_id": "ADJ-00054658"})
        assert doc is not None
        assert doc["last_run_id"] == "run-002"
        assert doc["file_hash"] == "a" * 64


class TestAttachments:
    def test_attachment_added_once_across_reruns(
        self, repo: MetadataRepository, mongo_database: mongomock.Database
    ) -> None:
        repo.upsert_record(make_record(identifier="EE47-1999"))
        attachment = AttachmentRef(
            url="https://www.workplacerelations.ie/en/Equality_Tribunal_Import/EE-1999-47.pdf",
            file_path="landing/body=1/partition=1999-12/EE47-1999__attachment_1.pdf",
            file_hash="e" * 64,
            content_type="application/pdf",
            file_size=59300,
        )
        seen = datetime(1999, 12, 15, tzinfo=UTC)
        repo.add_attachment("EE47-1999", attachment, "run-001", seen)
        repo.add_attachment("EE47-1999", attachment, "run-001", seen)

        doc = mongo_database["decisions_landing"].find_one({"_id": "EE47-1999"})
        assert doc is not None
        assert len(doc["attachments"]) == 1


class TestPartitionQueries:
    def test_iter_partition_filters_by_range_and_body(self, repo: MetadataRepository) -> None:
        repo.upsert_record(make_record())
        repo.upsert_record(
            make_record(
                identifier="LCR23157",
                body=Body.LABOUR_COURT,
                partition_date=datetime(2025, 6, 1, tzinfo=UTC),
                partition_key="2025-06",
            )
        )

        june_only = list(
            repo.iter_partition(
                datetime(2025, 6, 1, tzinfo=UTC),
                datetime(2025, 6, 30, tzinfo=UTC),
            )
        )
        assert [doc["_id"] for doc in june_only] == ["LCR23157"]

        wrc_only = list(
            repo.iter_partition(
                datetime(2025, 6, 1, tzinfo=UTC),
                datetime(2025, 7, 31, tzinfo=UTC),
                bodies=[int(Body.WORKPLACE_RELATIONS_COMMISSION)],
            )
        )
        assert [doc["_id"] for doc in wrc_only] == ["ADJ-00054658"]
