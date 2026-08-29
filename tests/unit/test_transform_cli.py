from datetime import date

import pytest

from wrc_pipeline.constants import Body
from wrc_pipeline.transform.__main__ import parse_args


def test_parse_args_dates_and_defaults() -> None:
    args = parse_args(["--start-date", "2025-01-01", "--end-date", "2025-06-30"])

    assert args.start_date == date(2025, 1, 1)
    assert args.end_date == date(2025, 6, 30)
    assert args.bodies is None
    assert args.max_workers == 8


def test_parse_args_bodies() -> None:
    args = parse_args(
        ["--start-date", "2025-01-01", "--end-date", "2025-06-30", "--bodies", "3,15376"]
    )

    assert args.bodies == [Body.LABOUR_COURT, Body.WORKPLACE_RELATIONS_COMMISSION]


def test_parse_args_rejects_inverted_range() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--start-date", "2025-06-30", "--end-date", "2025-01-01"])


def test_parse_args_rejects_bad_date() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--start-date", "01/06/2025", "--end-date", "2025-06-30"])
