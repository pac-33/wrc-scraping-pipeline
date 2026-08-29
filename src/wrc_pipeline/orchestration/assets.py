"""Monthly-partitioned assets: raw (ingestion) -> transformed (curated).

Both assets share one MonthlyPartitionsDefinition, so each transformed
partition depends on the same month of the raw asset — materializing
2025-06 of the transform requires 2025-06 of the raw decisions. Backfills
over a date range launch one run per partition, which pairs with the
run-queue limit (max_concurrent_runs=1) so a 24-month backfill never
becomes 24 concurrent crawlers against the site: the per-domain politeness
budget stays per-domain, not per-process.
"""

import os
import sys
from datetime import date, timedelta
from typing import Any

import dagster as dg

from wrc_pipeline.config import get_settings
from wrc_pipeline.transform.core import transform_range

PARTITIONS_START_ENV = "WRC_PARTITIONS_START"
DEFAULT_PARTITIONS_START = "2024-01-01"

monthly_partitions = dg.MonthlyPartitionsDefinition(
    start_date=os.environ.get(PARTITIONS_START_ENV, DEFAULT_PARTITIONS_START),
    end_offset=1,  # include the in-progress current month
)


def partition_window_to_spider_args(context: dg.AssetExecutionContext) -> tuple[str, str]:
    """Dagster time windows are [start, end) — the site's date filter (and our
    spider) is inclusive on both ends, so the exclusive end becomes last-day."""
    window = context.partition_time_window
    start = window.start.date()
    end = (window.end - timedelta(days=1)).date()
    return start.isoformat(), end.isoformat()


@dg.asset(
    partitions_def=monthly_partitions,
    group_name="wrc",
    kinds={"python", "scrapy"},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=60, backoff=dg.Backoff.EXPONENTIAL),
)
def raw_decisions(
    context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
) -> dg.MaterializeResult[None]:
    """One month of decisions scraped into the landing zone (Mongo + bucket).

    Runs the spider as a Pipes subprocess (fresh process per partition — the
    Twisted reactor is not restartable). The subprocess derives success from
    crawler stats and reports found/scraped/failed as materialization metadata.
    """
    start_date, end_date = partition_window_to_spider_args(context)
    return pipes_subprocess_client.run(
        command=[
            sys.executable,
            "-m",
            "wrc_pipeline.orchestration.run_spider",
            "--start-date",
            start_date,
            "--end-date",
            end_date,
        ],
        context=context,
    ).get_materialize_result()


@dg.asset(
    partitions_def=monthly_partitions,
    deps=[raw_decisions],
    group_name="wrc",
    kinds={"python"},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def transformed_decisions(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    """The same month, transformed into the curated zone (in-process — no
    reactor involved, so no subprocess needed). Thin wrapper over the same
    ``transform_range`` the CLI uses."""
    start_date, end_date = partition_window_to_spider_args(context)
    stats = transform_range(
        date.fromisoformat(start_date), date.fromisoformat(end_date), get_settings()
    )
    metadata: dict[str, Any] = {**stats.as_dict()}
    if stats.failures:
        metadata["failure_sample"] = str(stats.failures[:5])
        raise dg.Failure(
            description=f"{len(stats.failures)} record(s) failed to transform",
            metadata=metadata,
        )
    return dg.MaterializeResult(metadata=metadata)
