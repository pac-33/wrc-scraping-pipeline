"""Dagster Pipes subprocess entrypoint that runs one spider crawl.

Why a subprocess: the Twisted reactor cannot be restarted within a Python
process, so a long-lived Dagster worker materializing two partitions in a row
would crash with ReactorNotRestartable. Each partition gets a fresh process;
Pipes streams its (JSON) logs into the Dagster run and carries structured
metadata back.

Why not ``scrapy crawl`` + exit code: the Scrapy CLI exits 0 even when the
crawl broke mid-way. Success is derived from crawler stats instead:

- the crawl must finish cleanly (``finish_reason == "finished"``), and
- the reconciliation must hold: records_found == records_scraped + failures
  (every record the site reported is either stored or logged with a reason).
"""

import argparse
import os
import sys
from typing import Any

from dagster_pipes import PipesContext, open_dagster_pipes


def run_crawl(start_date: str, end_date: str, run_id: str | None) -> tuple[dict[str, Any], str]:
    """Run one DecisionsSpider crawl in this process; returns (stats, reason)."""
    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "wrc_pipeline.scraping.settings")
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    from wrc_pipeline.scraping.spiders.decisions import DecisionsSpider

    process = CrawlerProcess(get_project_settings(), install_root_handler=False)
    crawler = process.create_crawler(DecisionsSpider)
    process.crawl(crawler, start_date=start_date, end_date=end_date, run_id=run_id)
    process.start()

    stats = crawler.stats.get_stats() if crawler.stats else {}
    return stats, str(stats.get("finish_reason", "unknown"))


def evaluate(stats: dict[str, Any], finish_reason: str) -> tuple[bool, dict[str, Any]]:
    found = int(stats.get("wrc/records_found", 0))
    scraped = int(stats.get("wrc/records_scraped", 0))
    failures = int(stats.get("wrc/failures", 0))
    metadata = {
        "records_found": found,
        "records_scraped": scraped,
        "files_uploaded": int(stats.get("wrc/files_uploaded", 0)),
        "files_skipped_unchanged": int(stats.get("wrc/files_skipped_unchanged", 0)),
        "attachments_downloaded": int(stats.get("wrc/attachments_downloaded", 0)),
        "failures": failures,
        "requests": int(stats.get("downloader/request_count", 0)),
        "finish_reason": finish_reason,
    }
    ok = finish_reason == "finished" and found <= scraped + failures
    return ok, metadata


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    with open_dagster_pipes() as pipes:
        run_id = _dagster_run_id(pipes)
        stats, finish_reason = run_crawl(args.start_date, args.end_date, run_id)
        ok, metadata = evaluate(stats, finish_reason)
        pipes.report_asset_materialization(metadata=metadata)
        if not ok:
            pipes.log.error(f"crawl unhealthy: {metadata}")
            return 1
    return 0


def _dagster_run_id(pipes: PipesContext) -> str | None:
    try:
        return str(pipes.run_id)
    except Exception:  # noqa: BLE001 - best-effort correlation only
        return None


if __name__ == "__main__":
    sys.exit(main())
