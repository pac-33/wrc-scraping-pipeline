from datetime import UTC, date, datetime

import mongomock
import pytest

from tests.conftest import load_fixture
from wrc_pipeline.config import Settings
from wrc_pipeline.constants import Body
from wrc_pipeline.hashing import content_hash, file_hash
from wrc_pipeline.transform.core import transform_range

JUNE_PARTITION = datetime(2025, 6, 1, tzinfo=UTC)
PDF_BYTES = b"%PDF-1.4 a direct decision file"


def landing_record(
    identifier: str,
    file_path: str,
    raw: bytes,
    *,
    extension: str = ".html",
    body: int = int(Body.WORKPLACE_RELATIONS_COMMISSION),
    attachments: list | None = None,
) -> dict:
    is_html = extension == ".html"
    return {
        "_id": identifier,
        "title": identifier,
        "description": "A v B",
        "published_date": datetime(2025, 6, 10, tzinfo=UTC),
        "partition_date": JUNE_PARTITION,
        "partition_key": "2025-06",
        "body": body,
        "body_label": "Workplace Relations Commission",
        "source_page_url": "https://www.workplacerelations.ie/en/search/",
        "doc_url": f"https://www.workplacerelations.ie/en/cases/2025/june/{identifier.lower()}.html",
        "doc_kind": "html_page" if is_html else "file",
        "file_path": file_path,
        "file_hash": file_hash(raw),
        "content_hash": content_hash(raw, is_html=is_html),
        "content_type": "text/html; charset=utf-8" if is_html else "application/pdf",
        "file_size": len(raw),
        "file_extension": extension,
        "scraped_at": datetime(2025, 8, 1, tzinfo=UTC),
        "attachments": attachments or [],
    }


@pytest.fixture
def database(settings: Settings) -> mongomock.Database:
    client: mongomock.MongoClient = mongomock.MongoClient(tz_aware=True)
    return client[settings.mongo.database]


@pytest.fixture
def seeded(settings: Settings, database: mongomock.Database, s3_client) -> dict:
    """Two HTML records (one with a PDF attachment) + one direct-PDF record,
    seeded into mongomock + moto exactly as the ingestion would leave them."""
    landing_bucket = settings.object_store.landing_bucket
    s3_client.create_bucket(Bucket=landing_bucket)

    case_html = load_fixture("case_page_adj00053864.html")
    stub_html = load_fixture("case_page_ee47_1999_with_pdf.html")
    attachment_pdf = b"%PDF-1.4 legacy attachment bytes"

    records = [
        landing_record(
            "ADJ-00053864", "landing/body=15376/partition=2025-06/ADJ-00053864.html", case_html
        ),
        landing_record(
            "EE47-1999",
            "landing/body=1/partition=2025-06/EE47-1999.html",
            stub_html,
            body=int(Body.EQUALITY_TRIBUNAL),
            attachments=[
                {
                    "url": "https://www.workplacerelations.ie/en/Equality_Tribunal_Import/EE-1999-47.pdf",
                    "file_path": "landing/body=1/partition=2025-06/EE47-1999__attachment_1.pdf",
                    "file_hash": file_hash(attachment_pdf),
                    "content_type": "application/pdf",
                    "file_size": len(attachment_pdf),
                }
            ],
        ),
        landing_record(
            "LCR-DIRECT-1",
            "landing/body=3/partition=2025-06/LCR-DIRECT-1.pdf",
            PDF_BYTES,
            extension=".pdf",
            body=int(Body.LABOUR_COURT),
        ),
    ]
    payloads = {
        "landing/body=15376/partition=2025-06/ADJ-00053864.html": case_html,
        "landing/body=1/partition=2025-06/EE47-1999.html": stub_html,
        "landing/body=1/partition=2025-06/EE47-1999__attachment_1.pdf": attachment_pdf,
        "landing/body=3/partition=2025-06/LCR-DIRECT-1.pdf": PDF_BYTES,
    }
    for key, data in payloads.items():
        s3_client.put_object(Bucket=landing_bucket, Key=key, Body=data)
    database[settings.mongo.landing_collection].insert_many(records)
    return {"records": records, "payloads": payloads}


def run_transform(settings: Settings, database, s3_client, **kwargs):
    return transform_range(
        date(2025, 6, 1),
        date(2025, 6, 30),
        settings,
        database=database,
        s3_client=s3_client,
        max_workers=2,
        **kwargs,
    )


def curated_keys(s3_client, settings: Settings) -> set[str]:
    listing = s3_client.list_objects_v2(Bucket=settings.object_store.curated_bucket)
    return {entry["Key"] for entry in listing.get("Contents", [])}


class TestTransformRange:
    def test_files_renamed_to_identifier_ext_in_curated_bucket(
        self, settings: Settings, database, s3_client, seeded
    ) -> None:
        stats = run_transform(settings, database, s3_client)

        assert stats.records_selected == 3
        assert stats.records_transformed == 3
        assert stats.failures == []
        assert curated_keys(s3_client, settings) == {
            "curated/body=15376/partition=2025-06/ADJ-00053864.html",
            "curated/body=1/partition=2025-06/EE47-1999.html",
            "curated/body=1/partition=2025-06/EE47-1999__attachment_1.pdf",
            "curated/body=3/partition=2025-06/LCR-DIRECT-1.pdf",
        }

    def test_pdf_copied_byte_identical_html_reduced(
        self, settings: Settings, database, s3_client, seeded
    ) -> None:
        run_transform(settings, database, s3_client)

        pdf = s3_client.get_object(
            Bucket=settings.object_store.curated_bucket,
            Key="curated/body=3/partition=2025-06/LCR-DIRECT-1.pdf",
        )["Body"].read()
        assert pdf == PDF_BYTES  # spec: no transformation for pdf/doc

        html = s3_client.get_object(
            Bucket=settings.object_store.curated_bucket,
            Key="curated/body=15376/partition=2025-06/ADJ-00053864.html",
        )["Body"].read()
        assert len(html) < len(
            seeded["payloads"]["landing/body=15376/partition=2025-06/ADJ-00053864.html"]
        )
        assert b"ADJUDICATION OFFICER DECISION" in html
        assert b"Cookie Policy" not in html

    def test_curated_records_written_with_new_path_and_hash(
        self, settings: Settings, database, s3_client, seeded
    ) -> None:
        run_transform(settings, database, s3_client)
        curated = database[settings.mongo.curated_collection]

        assert curated.count_documents({}) == 3
        doc = curated.find_one({"_id": "ADJ-00053864"})
        assert doc is not None
        assert doc["file_path"] == "curated/body=15376/partition=2025-06/ADJ-00053864.html"
        assert doc["file_hash"] != seeded["records"][0]["file_hash"]  # new bytes, new hash
        assert doc["source_content_hash"] == seeded["records"][0]["content_hash"]
        assert doc["content_is_empty"] is False
        assert doc["extracted_title"].startswith("ADJ-00053864")

    def test_landing_zone_is_never_written(
        self, settings: Settings, database, s3_client, seeded
    ) -> None:
        before = {doc["_id"]: doc for doc in database[settings.mongo.landing_collection].find({})}
        run_transform(settings, database, s3_client)
        after = {doc["_id"]: doc for doc in database[settings.mongo.landing_collection].find({})}

        assert before == after

    def test_rerun_is_idempotent(self, settings: Settings, database, s3_client, seeded) -> None:
        run_transform(settings, database, s3_client)
        second = run_transform(settings, database, s3_client)

        assert second.records_transformed == 0
        assert second.records_skipped_unchanged == 3
        assert database[settings.mongo.curated_collection].count_documents({}) == 3

    def test_changed_source_is_retransformed(
        self, settings: Settings, database, s3_client, seeded
    ) -> None:
        run_transform(settings, database, s3_client)
        new_html = b'<html><body><div class="content"><h1>AMENDED</h1><p>New decision text here.</p></div></body></html>'
        s3_client.put_object(
            Bucket=settings.object_store.landing_bucket,
            Key="landing/body=15376/partition=2025-06/ADJ-00053864.html",
            Body=new_html,
        )
        database[settings.mongo.landing_collection].update_one(
            {"_id": "ADJ-00053864"},
            {"$set": {"content_hash": content_hash(new_html, is_html=True)}},
        )

        stats = run_transform(settings, database, s3_client)

        assert stats.records_transformed == 1
        assert stats.records_skipped_unchanged == 2
        html = s3_client.get_object(
            Bucket=settings.object_store.curated_bucket,
            Key="curated/body=15376/partition=2025-06/ADJ-00053864.html",
        )["Body"].read()
        assert b"AMENDED" in html

    def test_missing_object_fails_that_record_only(
        self, settings: Settings, database, s3_client, seeded
    ) -> None:
        s3_client.delete_object(
            Bucket=settings.object_store.landing_bucket,
            Key="landing/body=3/partition=2025-06/LCR-DIRECT-1.pdf",
        )

        stats = run_transform(settings, database, s3_client)

        assert stats.records_transformed == 2
        assert len(stats.failures) == 1
        assert stats.failures[0]["identifier"] == "LCR-DIRECT-1"

    def test_bodies_filter(self, settings: Settings, database, s3_client, seeded) -> None:
        stats = run_transform(settings, database, s3_client, bodies=[Body.LABOUR_COURT])

        assert stats.records_selected == 1
        assert stats.records_transformed == 1
