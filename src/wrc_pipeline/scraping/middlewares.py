"""Retry middleware with Retry-After compliance, backoff, and a circuit breaker.

Scrapy's stock RetryMiddleware re-schedules retries immediately and ignores
the Retry-After header (scrapy/scrapy#3849 is still open), which is the
opposite of etiquette on 429/503. This subclass:

- honors Retry-After (delta-seconds or HTTP-date), capped by config;
- otherwise applies exponential backoff with jitter per retry attempt;
- trips a circuit breaker after N *consecutive* retryable failures — a
  struggling server gets silence, and the orchestrator re-runs the partition
  later (idempotency makes the re-run nearly free).

The async sleep delays only the affected request's slot, not the whole crawl.
"""

import asyncio
import email.utils
import random
from datetime import UTC, datetime
from typing import Any

from scrapy import Request, Spider
from scrapy.crawler import Crawler
from scrapy.downloadermiddlewares.retry import RetryMiddleware, get_retry_request
from scrapy.exceptions import CloseSpider
from scrapy.http import Response
from scrapy.utils.response import response_status_message

from wrc_pipeline.config import get_settings


class RetryAfterBackoffMiddleware(RetryMiddleware):
    def __init__(self, crawler: Crawler) -> None:
        super().__init__(crawler.settings)
        self._crawler = crawler
        scraper = get_settings().scraper
        self._retry_after_cap = scraper.retry_after_cap_seconds
        self._backoff_base = scraper.backoff_base_seconds
        self._breaker_threshold = scraper.circuit_breaker_threshold
        self._consecutive_failures = 0

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "RetryAfterBackoffMiddleware":
        return cls(crawler)

    async def process_response(  # type: ignore[override]  # Scrapy supports coroutine middleware methods
        self, request: Request, response: Response, spider: Spider
    ) -> Request | Response:
        if request.meta.get("dont_retry", False):
            return response
        if response.status not in self.retry_http_codes:
            self._consecutive_failures = 0
            return response

        self._consecutive_failures += 1
        if self._consecutive_failures >= self._breaker_threshold:
            spider.logger.error(
                "circuit_breaker_tripped",
                extra={"consecutive_failures": self._consecutive_failures, "url": request.url},
            )
            raise CloseSpider("circuit_breaker_tripped")

        delay = self._retry_delay(request, response)
        if delay > 0:
            spider.logger.info(
                "retry_backoff",
                extra={
                    "url": request.url,
                    "http_status": response.status,
                    "delay_seconds": round(delay, 2),
                    "attempt": request.meta.get("retry_times", 0) + 1,
                },
            )
            await asyncio.sleep(delay)

        reason = response_status_message(response.status)
        retried = get_retry_request(request, spider=spider, reason=reason)
        if retried is not None:
            return retried
        # Retries exhausted: hand the response on so HttpErrorMiddleware routes
        # it to the request's errback, where the miss is logged with its reason.
        return response

    def _retry_delay(self, request: Request, response: Response) -> float:
        retry_after = _parse_retry_after(response.headers.get("Retry-After"))
        if retry_after is not None:
            return float(min(retry_after, self._retry_after_cap))
        attempt = request.meta.get("retry_times", 0)
        backoff: float = self._backoff_base * (2**attempt)
        return backoff * random.uniform(0.5, 1.5)


def _parse_retry_after(raw: Any) -> int | None:
    if raw is None:
        return None
    value = raw.decode("latin-1") if isinstance(raw, bytes) else str(raw)
    if value.isdigit():
        return int(value)
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    remaining = (parsed - datetime.now(tz=UTC)).total_seconds()
    return max(int(remaining), 0)
