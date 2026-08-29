"""Dagster code-location definitions (target of `dagster dev` / the code server)."""

import dagster as dg

from wrc_pipeline.config import get_settings
from wrc_pipeline.logging import setup_logging
from wrc_pipeline.orchestration.assets import (
    monthly_partitions,
    raw_decisions,
    transformed_decisions,
)

_settings = get_settings()
setup_logging(_settings.log_level, _settings.log_format)

wrc_pipeline_job = dg.define_asset_job(
    name="wrc_pipeline_job",
    selection=dg.AssetSelection.assets(raw_decisions, transformed_decisions),
    partitions_def=monthly_partitions,
)

defs = dg.Definitions(
    assets=[raw_decisions, transformed_decisions],
    jobs=[wrc_pipeline_job],
    resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
)
