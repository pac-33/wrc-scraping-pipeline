# WRC Scraping Pipeline

A scraping pipeline for [Workplace Relations](https://www.workplacerelations.ie/en/search/) decisions and determinations:
Scrapy ingestion → MongoDB metadata + MinIO document storage → BeautifulSoup transformation → Dagster orchestration.

Full run instructions land with the final PR. Development quickstart:

```bash
cp .env.example .env       # then edit credentials/contact email
docker compose up -d       # MongoDB + MinIO (+ bucket bootstrap)
uv sync                    # install dependencies (Python 3.13, uv)
uv run pytest              # run the test suite
```
