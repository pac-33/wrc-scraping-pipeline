"""CLI entrypoint: ``python -m wrc_pipeline.transform`` (or ``uv run wrc-transform``).

Thin wrapper over :func:`wrc_pipeline.transform.core.transform_range` — the
same function the Dagster asset calls, so the transformation runs identically
orchestrated or standalone.
"""

import argparse
import sys
from datetime import date

from wrc_pipeline.config import get_settings
from wrc_pipeline.constants import Body
from wrc_pipeline.logging import setup_logging
from wrc_pipeline.transform.core import transform_range


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="wrc-transform",
        description="Transform landing-zone documents into the curated zone for a date range.",
    )
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--bodies",
        type=lambda raw: [Body(int(part)) for part in raw.split(",") if part.strip()],
        default=None,
        help="Comma-separated body ids (default: all). E.g. --bodies 3,15376",
    )
    parser.add_argument("--max-workers", type=int, default=8)
    args = parser.parse_args(argv)
    if args.start_date > args.end_date:
        parser.error("--start-date must not be after --end-date")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)
    stats = transform_range(
        args.start_date,
        args.end_date,
        settings,
        bodies=args.bodies,
        max_workers=args.max_workers,
    )
    return 1 if stats.failures else 0


if __name__ == "__main__":
    sys.exit(main())
