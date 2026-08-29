"""Date-range partitioning for the scraper.

The site's date filter is inclusive on both ends (verified: a single-day
search returns records dated that day), so partitions span
[first-of-month, last-of-month] — half-open ranges would double-count
records falling on partition boundaries. The first and last partitions are
clamped to the requested range so the scraper fetches exactly what was asked.
"""

import calendar
from dataclasses import dataclass
from datetime import UTC, date, datetime

from wrc_pipeline.constants import SITE_DATE_FORMAT


@dataclass(frozen=True)
class Partition:
    """One scraping window, e.g. key='2025-06', 2025-06-01..2025-06-30."""

    key: str
    start: date
    end: date

    @property
    def query_from(self) -> str:
        return self.start.strftime(SITE_DATE_FORMAT)

    @property
    def query_to(self) -> str:
        return self.end.strftime(SITE_DATE_FORMAT)

    @property
    def partition_date(self) -> datetime:
        """Normalized partition marker: first of month, midnight UTC (BSON-friendly)."""
        return datetime(self.start.year, self.start.month, 1, tzinfo=UTC)


def month_partitions(start_date: date, end_date: date) -> list[Partition]:
    if start_date > end_date:
        msg = f"start_date {start_date} is after end_date {end_date}"
        raise ValueError(msg)

    partitions: list[Partition] = []
    cursor = date(start_date.year, start_date.month, 1)
    while cursor <= end_date:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        month_end = date(cursor.year, cursor.month, last_day)
        partitions.append(
            Partition(
                key=f"{cursor.year:04d}-{cursor.month:02d}",
                start=max(cursor, start_date),
                end=min(month_end, end_date),
            )
        )
        cursor = (
            date(cursor.year + 1, 1, 1)
            if cursor.month == 12
            else date(cursor.year, cursor.month + 1, 1)
        )
    return partitions
