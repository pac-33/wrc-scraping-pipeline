import itertools
from datetime import UTC, date, datetime

import pytest

from wrc_pipeline.scraping.partitions import month_partitions


def test_full_year_produces_twelve_partitions() -> None:
    partitions = month_partitions(date(2024, 1, 1), date(2024, 12, 31))

    assert len(partitions) == 12
    assert partitions[0].key == "2024-01"
    assert partitions[-1].key == "2024-12"
    assert partitions[1].query_from == "01/02/2024"
    assert partitions[1].query_to == "29/02/2024"  # leap year


def test_partitions_do_not_overlap_on_boundaries() -> None:
    """The site's date filter is inclusive on both ends, so consecutive
    partitions must not share a boundary day."""
    partitions = month_partitions(date(2025, 1, 1), date(2025, 3, 31))

    for previous, current in itertools.pairwise(partitions):
        assert previous.end < current.start


def test_partial_months_are_clamped_to_requested_range() -> None:
    partitions = month_partitions(date(2025, 1, 15), date(2025, 2, 10))

    assert [p.key for p in partitions] == ["2025-01", "2025-02"]
    assert partitions[0].query_from == "15/01/2025"
    assert partitions[0].query_to == "31/01/2025"
    assert partitions[1].query_from == "01/02/2025"
    assert partitions[1].query_to == "10/02/2025"


def test_single_day_range() -> None:
    partitions = month_partitions(date(2025, 6, 5), date(2025, 6, 5))

    assert len(partitions) == 1
    assert partitions[0].query_from == partitions[0].query_to == "05/06/2025"


def test_cross_year_range() -> None:
    partitions = month_partitions(date(2024, 11, 1), date(2025, 2, 28))

    assert [p.key for p in partitions] == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_partition_date_is_first_of_month_utc() -> None:
    (partition,) = month_partitions(date(2025, 6, 15), date(2025, 6, 20))

    assert partition.partition_date == datetime(2025, 6, 1, tzinfo=UTC)


def test_inverted_range_raises() -> None:
    with pytest.raises(ValueError, match="after"):
        month_partitions(date(2025, 2, 1), date(2025, 1, 1))
