"""MongoDB access: client factory, index bootstrap, and the metadata repository.

The repository is the only code that touches collections directly; spiders,
pipelines and the transformation call it through this interface. Records use
the natural business key as ``_id`` (the decision identifier), which makes
duplicate prevention a property of the primary-key index and lets upserts
filter on ``_id`` — the access pattern MongoDB recommends to avoid the
concurrent-upsert duplicate-key race.
"""

from collections.abc import Iterator
from datetime import datetime
from typing import Any

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from wrc_pipeline.config import MongoSettings
from wrc_pipeline.models import AttachmentRef, DecisionRecord, RunReport

Document = dict[str, Any]


def create_mongo_client(settings: MongoSettings) -> MongoClient[Document]:
    # tz_aware so datetimes round-trip as timezone-aware UTC instead of naive.
    return MongoClient(
        settings.uri.get_secret_value(),
        tz_aware=True,
        serverSelectionTimeoutMS=5_000,
    )


def get_database(client: MongoClient[Document], settings: MongoSettings) -> Database[Document]:
    return client[settings.database]


def ensure_indexes(db: Database[Document], settings: MongoSettings) -> None:
    """Idempotent index bootstrap, called at process start (create_index is a
    no-op when the index already exists). Kept in code — Mongo initdb scripts
    only run on empty volumes, which is exactly when you'd forget them."""
    for name in (settings.landing_collection, settings.curated_collection):
        db[name].create_index(
            [("partition_date", ASCENDING), ("body", ASCENDING)],
            name="ix_partition_body",
        )
    db[settings.runs_collection].create_index(
        [("run_id", ASCENDING)], name="uq_run_id", unique=True
    )


class MetadataRepository:
    """Repository over one decisions collection (landing or curated)."""

    def __init__(self, collection: Collection[Document]) -> None:
        self._collection = collection

    def get_content_hash(self, identifier: str) -> str | None:
        doc = self._collection.find_one({"_id": identifier}, projection={"content_hash": 1})
        return doc.get("content_hash") if doc else None

    def upsert_record(self, record: DecisionRecord) -> bool:
        """Insert or refresh a record; returns True when newly inserted.

        ``$set`` refreshes everything re-derivable from the current scrape;
        ``$setOnInsert`` pins first-seen provenance so re-runs never rewrite
        history. Retried once on the documented E11000 upsert race.
        """
        doc = record.to_document()
        identifier = doc.pop("identifier")
        run_id = doc.pop("run_id")
        update = {
            "$set": {**doc, "last_run_id": run_id, "last_seen_at": record.scraped_at},
            "$setOnInsert": {"first_seen_at": record.scraped_at, "first_run_id": run_id},
        }
        try:
            result = self._collection.update_one({"_id": identifier}, update, upsert=True)
        except DuplicateKeyError:
            result = self._collection.update_one({"_id": identifier}, update, upsert=True)
        return result.upserted_id is not None

    def touch_unchanged(self, identifier: str, run_id: str, seen_at: datetime) -> None:
        """Mark an unchanged record as seen by this run without rewriting it."""
        self._collection.update_one(
            {"_id": identifier},
            {"$set": {"last_seen_at": seen_at, "last_run_id": run_id}},
        )

    def add_attachment(self, identifier: str, attachment: AttachmentRef) -> None:
        """Attach a linked file to its parent record. ``$addToSet`` keys on the
        full sub-document, so re-runs with identical content do not duplicate;
        upsert covers the rare case where the attachment lands before the
        parent record (the parent upsert later fills the remaining fields)."""
        try:
            self._collection.update_one(
                {"_id": identifier},
                {"$addToSet": {"attachments": attachment.model_dump()}},
                upsert=True,
            )
        except DuplicateKeyError:
            self._collection.update_one(
                {"_id": identifier},
                {"$addToSet": {"attachments": attachment.model_dump()}},
            )

    def iter_partition(
        self,
        partition_start: datetime,
        partition_end: datetime,
        bodies: list[int] | None = None,
    ) -> Iterator[Document]:
        query: Document = {"partition_date": {"$gte": partition_start, "$lte": partition_end}}
        if bodies:
            query["body"] = {"$in": bodies}
        return self._collection.find(query).sort(
            [("partition_date", ASCENDING), ("_id", ASCENDING)]
        )

    def count_partition(
        self,
        partition_start: datetime,
        partition_end: datetime,
        bodies: list[int] | None = None,
    ) -> int:
        query: Document = {"partition_date": {"$gte": partition_start, "$lte": partition_end}}
        if bodies:
            query["body"] = {"$in": bodies}
        return self._collection.count_documents(query)


class RunReportStore:
    def __init__(self, collection: Collection[Document]) -> None:
        self._collection = collection

    def save(self, report: RunReport) -> None:
        doc = report.model_dump()
        run_id = doc.pop("run_id")
        self._collection.update_one({"run_id": run_id}, {"$set": doc}, upsert=True)

    def get(self, run_id: str) -> Document | None:
        return self._collection.find_one({"run_id": run_id})

    def latest_for_partition(self, partition_key: str) -> Document | None:
        return self._collection.find_one(
            {"partitions": partition_key, "finished_at": {"$ne": None}},
            sort=[("finished_at", -1)],
        )
