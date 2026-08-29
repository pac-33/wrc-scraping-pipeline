import boto3
import mongomock
import pytest
from moto import mock_aws

from wrc_pipeline.config import MongoSettings, ObjectStoreSettings, ScraperSettings, Settings


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
