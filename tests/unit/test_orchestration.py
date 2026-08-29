from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg

from wrc_pipeline.orchestration.assets import partition_window_to_spider_args
from wrc_pipeline.orchestration.definitions import defs
from wrc_pipeline.orchestration.run_spider import evaluate


class TestDefinitions:
    def test_assets_and_job_load(self) -> None:
        asset_keys = {spec.key.to_user_string() for spec in defs.resolve_all_asset_specs()}

        assert asset_keys == {"raw_decisions", "transformed_decisions"}
        assert defs.resolve_job_def("wrc_pipeline_job") is not None

    def test_assets_share_monthly_partitions(self) -> None:
        specs = {spec.key.to_user_string(): spec for spec in defs.resolve_all_asset_specs()}
        raw = specs["raw_decisions"]
        transformed = specs["transformed_decisions"]

        assert isinstance(raw.partitions_def, dg.MonthlyPartitionsDefinition)
        assert transformed.partitions_def == raw.partitions_def
        assert {dep.asset_key.to_user_string() for dep in transformed.deps} == {"raw_decisions"}


class TestPartitionWindowMapping:
    def make_context(self, start: datetime, end: datetime):
        return SimpleNamespace(partition_time_window=SimpleNamespace(start=start, end=end))

    def test_exclusive_window_end_becomes_inclusive_last_day(self) -> None:
        """Dagster windows are [1st, 1st-of-next-month); the site filter is
        inclusive, so June's window must become 2025-06-01..2025-06-30."""
        context = self.make_context(
            datetime(2025, 6, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)
        )

        assert partition_window_to_spider_args(context) == ("2025-06-01", "2025-06-30")

    def test_february_leap_year(self) -> None:
        context = self.make_context(
            datetime(2024, 2, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC)
        )

        assert partition_window_to_spider_args(context) == ("2024-02-01", "2024-02-29")


class TestCrawlEvaluation:
    def base_stats(self) -> dict:
        return {
            "wrc/records_found": 30,
            "wrc/records_scraped": 30,
            "wrc/files_uploaded": 30,
            "wrc/failures": 0,
            "downloader/request_count": 35,
        }

    def test_clean_run_passes(self) -> None:
        ok, metadata = evaluate(self.base_stats(), "finished")

        assert ok is True
        assert metadata["records_found"] == metadata["records_scraped"] == 30

    def test_partial_failures_still_pass_when_accounted_for(self) -> None:
        """Spec: scraping 200-X is acceptable when every X is logged — the
        reconciliation found == scraped + failures encodes exactly that."""
        stats = self.base_stats() | {"wrc/records_scraped": 28, "wrc/failures": 2}

        ok, metadata = evaluate(stats, "finished")

        assert ok is True
        assert metadata["failures"] == 2

    def test_unaccounted_records_fail(self) -> None:
        stats = self.base_stats() | {"wrc/records_scraped": 25, "wrc/failures": 0}

        ok, _ = evaluate(stats, "finished")

        assert ok is False

    def test_circuit_breaker_close_fails(self) -> None:
        ok, metadata = evaluate(self.base_stats(), "circuit_breaker_tripped")

        assert ok is False
        assert metadata["finish_reason"] == "circuit_breaker_tripped"
