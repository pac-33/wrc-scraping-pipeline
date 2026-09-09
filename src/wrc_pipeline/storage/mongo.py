"""MongoDB access: client factories, index bootstrap, and the repositories.

The repositories are the only code that touches collections directly; the
scraper's pipeline, the transformation and tooling call them through this
interface. Records use the natural business key as ``_id`` (the decision
identifier), which makes duplicate prevention a property of the primary-key
index and lets upserts filter on ``_id`` — the access pattern MongoDB
recommends to avoid the concurrent-upsert duplicate-key race.

Two flavours share one update logic. The synchronous repositories back the
thread-pooled transformation and tooling. The asynchronous ones, on PyMongo's
async client (stable since 4.13), back Scrapy's coroutine item pipeline, so a
Mongo round trip suspends only the item waiting for it instead of the whole
crawl.
"""

from collections.abc import Iterator
from datetime import datetime
from typing import Any

from pymongo import ASCENDING, AsyncMongoClient, MongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from wrc_pipeline.config import MongoSettings
from wrc_pipeline.models import AttachmentRef, DecisionRecord, RunReport

Document = dict[str, Any]

_PARTITION_INDEX = [("partition_date", ASCENDING), ("body", ASCENDING)]
_RUN_ID_INDEX = [("run_id", ASCENDING)]


def create_mongo_client(settings: MongoSettings) -> MongoClient[Document]:
    # tz_aware so datetimes round-trip as timezone-aware UTC instead of naive.
    return MongoClient(
        settings.uri.get_secret_value(),
        tz_aware=True,
        serverSelectionTimeoutMS=5_000,
    )


def create_async_mongo_client(settings: MongoSettings) -> AsyncMongoClient[Document]:
    """Same options as the sync client, on PyMongo's asyncio-native client."""
    return AsyncMongoClient(
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
        db[name].create_index(_PARTITION_INDEX, name="ix_partition_body")
    db[settings.runs_collection].create_index(_RUN_ID_INDEX, name="uq_run_id", unique=True)


async def ensure_indexes_async(db: AsyncDatabase[Document], settings: MongoSettings) -> None:
    for name in (settings.landing_collection, settings.curated_collection):
        await db[name].create_index(_PARTITION_INDEX, name="ix_partition_body")
    await db[settings.runs_collection].create_index(_RUN_ID_INDEX, name="uq_run_id", unique=True)


def _upsert_update(record: DecisionRecord) -> tuple[str, Document]:
    """The (_id, update) pair for a landing record, shared by both repositories.

    ``$set`` refreshes everything re-derivable from the current scrape;
    ``$setOnInsert`` pins first-seen provenance so re-runs never rewrite history.
    """
    doc = record.to_document()
    identifier = doc.pop("identifier")
    run_id = doc.pop("run_id")
    update = {
        "$set": {**doc, "last_run_id": run_id, "last_seen_at": record.scraped_at},
        "$setOnInsert": {"first_seen_at": record.scraped_at, "first_run_id": run_id},
    }
    return identifier, update


class MetadataRepository:
    """Repository over one decisions collection (landing or curated)."""

    def __init__(self, collection: Collection[Document]) -> None:
        self._collection = collection

    def get_file_hash(self, identifier: str) -> str | None:
        doc = self._collection.find_one({"_id": identifier}, projection={"file_hash": 1})
        return doc.get("file_hash") if doc else None

    def upsert_record(self, record: DecisionRecord) -> bool:
        """Insert or refresh a record; returns True when newly inserted.
        Retried once on the documented E11000 upsert race."""
        identifier, update = _upsert_update(record)
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


class AsyncMetadataRepository:
    """The landing-collection writes as coroutines, for the item pipeline."""

    def __init__(self, collection: AsyncCollection[Document]) -> None:
        self._collection = collection

    async def get_file_hash(self, identifier: str) -> str | None:
        doc = await self._collection.find_one({"_id": identifier}, projection={"file_hash": 1})
        return doc.get("file_hash") if doc else None

    async def upsert_record(self, record: DecisionRecord) -> bool:
        identifier, update = _upsert_update(record)
        try:
            result = await self._collection.update_one({"_id": identifier}, update, upsert=True)
        except DuplicateKeyError:
            result = await self._collection.update_one({"_id": identifier}, update, upsert=True)
        return result.upserted_id is not None

    async def touch_unchanged(self, identifier: str, run_id: str, seen_at: datetime) -> None:
        await self._collection.update_one(
            {"_id": identifier},
            {"$set": {"last_seen_at": seen_at, "last_run_id": run_id}},
        )

    async def add_attachment(self, identifier: str, attachment: AttachmentRef) -> None:
        update = {"$addToSet": {"attachments": attachment.model_dump()}}
        try:
            await self._collection.update_one({"_id": identifier}, update, upsert=True)
        except DuplicateKeyError:
            await self._collection.update_one({"_id": identifier}, update)


class CuratedRepository:
    """Repository over the curated collection written by the transformation.

    Mirrors the landing repository's idempotency model: ``source_file_hash``
    records which landing content a curated record was derived from, so an
    unchanged source short-circuits re-transformation.
    """

    def __init__(self, collection: Collection[Document]) -> None:
        self._collection = collection

    def get_source_hash(self, identifier: str) -> str | None:
        doc = self._collection.find_one({"_id": identifier}, projection={"source_file_hash": 1})
        return doc.get("source_file_hash") if doc else None

    def upsert(self, identifier: str, doc: Document, first_seen_at: datetime) -> bool:
        update = {
            "$set": doc,
            "$setOnInsert": {"first_transformed_at": first_seen_at},
        }
        try:
            result = self._collection.update_one({"_id": identifier}, update, upsert=True)
        except DuplicateKeyError:
            result = self._collection.update_one({"_id": identifier}, update, upsert=True)
        return result.upserted_id is not None

    def count(self) -> int:
        return self._collection.count_documents({})


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


class AsyncRunReportStore:
    def __init__(self, collection: AsyncCollection[Document]) -> None:
        self._collection = collection

    async def save(self, report: RunReport) -> None:
        doc = report.model_dump()
        run_id = doc.pop("run_id")
        await self._collection.update_one({"run_id": run_id}, {"$set": doc}, upsert=True)
