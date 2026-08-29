import pytest
from pydantic import ValidationError

from wrc_pipeline.config import Settings
from wrc_pipeline.constants import Body


def _base_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WRC_MONGO__URI", "mongodb://u:p@somehost:27017")
    monkeypatch.setenv("WRC_OBJECT_STORE__ACCESS_KEY", "ak")
    monkeypatch.setenv("WRC_OBJECT_STORE__SECRET_KEY", "sk")


def test_nested_env_vars_resolve(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    monkeypatch.setenv("WRC_MONGO__DATABASE", "custom_db")
    monkeypatch.setenv("WRC_OBJECT_STORE__ENDPOINT_URL", "http://minio.internal:9000")
    monkeypatch.setenv("WRC_SCRAPER__TARGET_CONCURRENCY", "1.0")

    settings = Settings(_env_file=None)

    assert settings.mongo.database == "custom_db"
    assert settings.object_store.endpoint_url == "http://minio.internal:9000"
    assert settings.scraper.target_concurrency == 1.0


def test_missing_required_settings_fail_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WRC_MONGO__URI", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_secrets_are_redacted_in_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    settings = Settings(_env_file=None)

    rendered = repr(settings.mongo) + repr(settings.object_store)

    assert "somehost" not in rendered
    assert "sk" * 1 not in rendered.replace("SecretStr", "")
    assert settings.mongo.uri.get_secret_value() == "mongodb://u:p@somehost:27017"


def test_scraper_defaults_cover_all_bodies(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    settings = Settings(_env_file=None)

    assert settings.scraper.bodies == list(Body)
    assert settings.scraper.partition_size == "monthly"


def test_bodies_override_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    monkeypatch.setenv("WRC_SCRAPER__BODIES", "[3, 15376]")

    settings = Settings(_env_file=None)

    assert settings.scraper.bodies == [Body.LABOUR_COURT, Body.WORKPLACE_RELATIONS_COMMISSION]
