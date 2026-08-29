import json
import logging

import pytest

from wrc_pipeline.logging import get_logger, setup_logging


@pytest.fixture(autouse=True)
def _reset_root_handlers():
    yield
    logging.getLogger().handlers.clear()


def test_own_logs_render_as_json_lines(capsys: pytest.CaptureFixture[str]) -> None:
    setup_logging(level="INFO", log_format="json")
    get_logger("wrc_pipeline.test").info(
        "partition_started", partition="2025-06", body_label="Labour Court"
    )

    line = capsys.readouterr().out.strip().splitlines()[-1]
    event = json.loads(line)
    assert event["event"] == "partition_started"
    assert event["partition"] == "2025-06"
    assert event["body_label"] == "Labour Court"
    assert event["level"] == "info"
    assert "timestamp" in event


def test_third_party_stdlib_logs_render_as_json_too(capsys: pytest.CaptureFixture[str]) -> None:
    """Scrapy/pymongo/boto3 log through stdlib logging — their records must
    come out as the same JSON stream via the foreign_pre_chain."""
    setup_logging(level="INFO", log_format="json")
    logging.getLogger("scrapy.core.engine").info("Spider opened: %s", "decisions")

    line = capsys.readouterr().out.strip().splitlines()[-1]
    event = json.loads(line)
    assert event["event"] == "Spider opened: decisions"
    assert event["logger"] == "scrapy.core.engine"


def test_extra_fields_from_stdlib_loggers_reach_the_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """spider.logger.info("event", extra={...}) is a stdlib call — ExtraAdder
    must lift those fields into the rendered JSON event."""
    setup_logging(level="INFO", log_format="json")
    logging.getLogger("decisions").info(
        "partition_search_started", extra={"partition": "2025-06", "total": 192}
    )

    line = capsys.readouterr().out.strip().splitlines()[-1]
    event = json.loads(line)
    assert event["partition"] == "2025-06"
    assert event["total"] == 192


def test_noisy_libraries_are_quieted(capsys: pytest.CaptureFixture[str]) -> None:
    setup_logging(level="INFO", log_format="json")
    logging.getLogger("pymongo.topology").info("heartbeat noise")

    assert "heartbeat noise" not in capsys.readouterr().out
