from pathlib import Path
from typing import Any

import boto3
import mongomock
import pytest
from moto import mock_aws

from wrc_pipeline.config import (
    MongoSettings,
    ObjectStoreSettings,
    ScraperSettings,
    Settings,
    get_settings,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _app_env(monkeypatch: pytest.MonkeyPatch):
    """Baseline env so code paths calling get_settings() work in unit tests."""
    monkeypatch.setenv("WRC_MONGO__URI", "mongodb://user:pass@localhost:27017")
    monkeypatch.setenv("WRC_OBJECT_STORE__ACCESS_KEY", "test-key")
    monkeypatch.setenv("WRC_OBJECT_STORE__SECRET_KEY", "test-secret")
    monkeypatch.setenv("WRC_SCRAPER__CONTACT_EMAIL", "pipeline@test.local")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        mongo=MongoSettings(uri="mongodb://user:pass@localhost:27017"),
        object_store=ObjectStoreSettings(access_key="test-key", secret_key="test-secret"),
        scraper=ScraperSettings(contact_email="pipeline@test.local"),
    )


@pytest.fixture
def mongo_database(settings: Settings) -> mongomock.Database:
    client: mongomock.MongoClient = mongomock.MongoClient(tz_aware=True)
    return client[settings.mongo.database]


@pytest.fixture
def s3_client():
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


class AsyncCollectionShim:
    """mongomock is synchronous; expose the calls the async repositories await."""

    def __init__(self, collection: mongomock.Collection) -> None:
        self._collection = collection

    async def find_one(self, *args: Any, **kwargs: Any) -> Any:
        return self._collection.find_one(*args, **kwargs)

    async def update_one(self, *args: Any, **kwargs: Any) -> Any:
        return self._collection.update_one(*args, **kwargs)

    async def create_index(self, *args: Any, **kwargs: Any) -> Any:
        return self._collection.create_index(*args, **kwargs)


class AsyncClientShim:
    async def close(self) -> None:
        return None


def load_fixture(name: str) -> bytes:
    return (FIXTURES_DIR / name).read_bytes()
