from datetime import UTC, datetime
from unittest.mock import MagicMock

import mongomock
import pytest
from pymongo.errors import DuplicateKeyError

from wrc_pipeline.constants import Body
from wrc_pipeline.models import AttachmentRef, DecisionRecord, DocKind
from wrc_pipeline.storage.mongo import MetadataRepository


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
        "content_hash": "b" * 64,
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
        repo.upsert_record(make_record(content_hash="c" * 64, file_hash="d" * 64))

        doc = mongo_database["decisions_landing"].find_one({"_id": "ADJ-00054658"})
        assert doc is not None
        assert doc["content_hash"] == "c" * 64

    def test_duplicate_key_race_is_retried_once(self) -> None:
        collection = MagicMock()
        ok_result = MagicMock(upserted_id=None)
        collection.update_one.side_effect = [DuplicateKeyError("E11000"), ok_result]

        repo = MetadataRepository(collection)
        inserted = repo.upsert_record(make_record())

        assert inserted is False
        assert collection.update_one.call_count == 2


class TestChangeDetection:
    def test_get_content_hash_roundtrip(self, repo: MetadataRepository) -> None:
        assert repo.get_content_hash("ADJ-00054658") is None
        repo.upsert_record(make_record())
        assert repo.get_content_hash("ADJ-00054658") == "b" * 64

    def test_touch_unchanged_updates_only_seen_markers(
        self, repo: MetadataRepository, mongo_database: mongomock.Database
    ) -> None:
        repo.upsert_record(make_record())
        repo.touch_unchanged("ADJ-00054658", "run-002", datetime(2025, 8, 2, tzinfo=UTC))

        doc = mongo_database["decisions_landing"].find_one({"_id": "ADJ-00054658"})
        assert doc is not None
        assert doc["last_run_id"] == "run-002"
        assert doc["content_hash"] == "b" * 64


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
        repo.add_attachment("EE47-1999", attachment)
        repo.add_attachment("EE47-1999", attachment)

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
