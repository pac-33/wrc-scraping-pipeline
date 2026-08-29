# WRC Scraping Pipeline

A scraping pipeline for Ireland's [Workplace Relations](https://www.workplacerelations.ie/en/search/) Decisions & Determinations database:

**Scrapy** ingestion → **MongoDB** metadata + **MinIO** document storage (landing zone) → **BeautifulSoup** transformation (curated zone) → **Dagster** orchestration with monthly partitions.

Design rationale (partition size, rate limiting, retries, dedup, 50+ sources) lives in [ARCHITECTURE.md](ARCHITECTURE.md).

## Prerequisites

- Docker (with Compose v2)
- [uv](https://docs.astral.sh/uv/) — installs Python 3.13 and all dependencies itself

## Setup

```bash
cp .env.example .env
# edit .env: set real passwords and your contact email (goes into the User-Agent)

docker compose up -d      # MongoDB + MinIO + bucket bootstrap
uv sync                   # create venv, install locked dependencies
```

All configuration is environment-driven (`WRC_*` variables, see `.env.example`): connection strings, bucket/collection names, bodies, partition size, politeness knobs, retry policy, and the robots-exception toggle for legacy `/..._Import/` decision PDFs. Nothing is hardcoded.

## Run the scraper (ingestion → landing zone)

```bash
uv run scrapy crawl decisions -a start_date=2025-01-01 -a end_date=2025-06-30
```

- `start_date` / `end_date` (required): `YYYY-MM-DD` or `YYYY-MM`. The range is split into monthly partitions; every record gets a `partition_date`.
- `bodies` (optional): comma-separated body ids to restrict the crawl, e.g. `-a bodies=3,15376` (1 = Equality Tribunal, 2 = Employment Appeals Tribunal, 3 = Labour Court, 15376 = WRC). Default: all four.

What lands where:

- Documents → MinIO bucket `wrc-landing`, keys like `landing/body=15376/partition=2025-06/ADJ-00054658.html` (HTML case pages as `.html`; directly-linked PDFs/DOCs as-is; legacy in-page decision PDFs as `…__attachment_N.pdf`).
- Metadata → Mongo collection `decisions_landing` (one record per decision: title, description, dates, body, URLs, `file_path`, sha256 `file_hash` + canonical `content_hash`, provenance).
- A run report → `pipeline_runs` (found / scraped / uploaded / skipped-unchanged / failures with reasons).

Logs are JSON lines (Scrapy internals included). Re-running the same range is **idempotent**: no duplicate records, unchanged files are not re-uploaded (`files_skipped_unchanged` in the summary).

## Run the transformation (landing → curated zone)

```bash
uv run wrc-transform --start-date 2025-01-01 --end-date 2025-06-30
```

For every landing record in the range: PDF/DOC copied byte-identical, HTML reduced to the decision's relevant content (site chrome stripped), **all files renamed to `identifier.ext`** in `wrc-curated`, and a curated record (new path, new hash, extraction-quality fields) upserted into `decisions_curated`. The landing zone is never modified. Idempotent via `source_content_hash`.

Options: `--bodies 3,15376`, `--max-workers 8`.

## Orchestrate with Dagster

Local (uses your `.env` / shell environment):

```bash
uv run dagster dev
```

Open http://localhost:3000 → assets `raw_decisions` → `transformed_decisions`, monthly partitions. Materialize a partition, or **Materialize → backfill** over a date range; runs queue one at a time (politeness is per-domain, not per-process). Partition metadata shows found/scraped/uploaded/skipped/failures per month.

Fully containerized deployment (webserver + daemon + code server + Postgres run storage):

```bash
docker compose --profile dagster up -d --build
```

Open http://localhost:3000 (override with `WRC_DAGSTER_HOST_PORT`). The first month covered by the partition set is `WRC_PARTITIONS_START` (default `2024-01-01`).

Command-line materialization of one partition:

```bash
uv run dagster asset materialize -m wrc_pipeline.orchestration.definitions --select raw_decisions --partition 2025-06-01
uv run dagster asset materialize -m wrc_pipeline.orchestration.definitions --select transformed_decisions --partition 2025-06-01
```

## Verified against the live site

January–June 2025, all four bodies (1,801 HTTP requests, ~9.4 min at ~3 req/s):

| Run | found | scraped | uploaded | skipped unchanged | failures |
|---|---|---|---|---|---|
| ingest | 1,621 | 1,621 | 1,591 | 30¹ | 0 |
| ingest again (idempotency) | 1,621 | 1,621 | 5² | 1,616 | 0 |
| transform | 1,618³ selected | 1,588 transformed | — | 30¹ | 0 |
| transform again | 1,618 selected | 0 | — | 1,618 | 0 |

¹ Documents already landed by an earlier smoke run of the same month.
² Three decisions are listed twice by the site under one identifier with two live URLs of differing content (e.g. `adj-00052577.html` and an amended `adj-000525771.html`); each run converges the record to the last-fetched version. Every other document was recognised as unchanged despite the server's volatile `<!-- Elapsed time -->` comment — see the dual-hash design in [ARCHITECTURE.md](ARCHITECTURE.md).
³ 1,621 scraped items minus those 3 site-side duplicate listings = 1,618 distinct records.

## Tests & quality gates

```bash
uv run pytest            # 108 tests, offline (captured real pages as fixtures)
uv run ruff check .      # lint
uv run ruff format .     # format
uv run mypy              # strict typing on first-party code
```

CI (GitHub Actions) runs all four on every PR.

## Project layout

```
src/wrc_pipeline/
├── config.py            # pydantic-settings: every tunable via WRC_* env vars
├── constants.py         # site facts (body ids, URL scheme, magic bytes)
├── logging.py           # structlog: one JSON stream, third-party logs included
├── models.py            # DecisionRecord / AttachmentRef / RunReport (validation)
├── hashing.py           # file_hash + canonicalized content_hash (change detection)
├── naming.py            # identifier sanitization, type detection, object keys
├── storage/             # Mongo repositories + S3-compatible object store
├── scraping/            # Scrapy: spider, retry middleware, pipelines, settings
├── transform/           # BeautifulSoup extraction + curated writer + CLI
└── orchestration/       # Dagster assets, definitions, Pipes spider runner
```

## Useful consoles

- MinIO: http://localhost:9001 (credentials from `.env`)
- Mongo: `docker compose exec mongodb mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD"`

## Notes

- The image pins the last full-console community release of MinIO (the OSS project was frozen upstream in 2026); the code talks generic S3 through boto3, so moving to AWS S3 (or any S3-compatible store) is an `WRC_OBJECT_STORE__ENDPOINT_URL` change.
- robots.txt compliance, the documented `/..._Import/` exception, and the politeness numbers are covered in [ARCHITECTURE.md](ARCHITECTURE.md).
