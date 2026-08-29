import asyncio
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest
from scrapy import Request
from scrapy.exceptions import CloseSpider
from scrapy.http import Response
from scrapy.utils.test import get_crawler

from wrc_pipeline.config import get_settings
from wrc_pipeline.scraping.middlewares import RetryAfterBackoffMiddleware, _parse_retry_after
from wrc_pipeline.scraping.spiders.decisions import DecisionsSpider


@pytest.fixture
def spider() -> DecisionsSpider:
    crawler = get_crawler(DecisionsSpider, {"RETRY_TIMES": 2})
    return DecisionsSpider.from_crawler(crawler, start_date="2025-06", end_date="2025-06")


@pytest.fixture
def middleware(spider: DecisionsSpider) -> RetryAfterBackoffMiddleware:
    return RetryAfterBackoffMiddleware.from_crawler(spider.crawler)


@pytest.fixture
def sleep_calls(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        calls.append(seconds)

    monkeypatch.setattr("wrc_pipeline.scraping.middlewares.asyncio.sleep", fake_sleep)
    return calls


def process(middleware: RetryAfterBackoffMiddleware, request: Request, response: Response, spider):
    return asyncio.run(middleware.process_response(request, response, spider))


def make_response(
    status: int,
    url: str = "https://www.workplacerelations.ie/en/search/",
    headers: dict | None = None,
) -> Response:
    return Response(url=url, status=status, headers=headers or {}, request=Request(url))


class TestRetryAfter:
    def test_success_passes_through_and_resets_breaker(
        self, middleware: RetryAfterBackoffMiddleware, spider: DecisionsSpider, sleep_calls
    ) -> None:
        request = Request("https://www.workplacerelations.ie/en/search/")
        middleware._consecutive_failures = 5

        result = process(middleware, request, make_response(200), spider)

        assert isinstance(result, Response)
        assert middleware._consecutive_failures == 0
        assert sleep_calls == []

    def test_retry_after_seconds_header_is_honored(
        self, middleware: RetryAfterBackoffMiddleware, spider: DecisionsSpider, sleep_calls
    ) -> None:
        request = Request("https://www.workplacerelations.ie/en/search/")
        response = make_response(429, headers={"Retry-After": "7"})

        result = process(middleware, request, response, spider)

        assert sleep_calls == [7.0]
        assert isinstance(result, Request)
        assert result.meta["retry_times"] == 1

    def test_retry_after_is_capped(
        self, middleware: RetryAfterBackoffMiddleware, spider: DecisionsSpider, sleep_calls
    ) -> None:
        request = Request("https://www.workplacerelations.ie/en/search/")
        response = make_response(503, headers={"Retry-After": "99999"})

        process(middleware, request, response, spider)

        assert sleep_calls == [float(get_settings().scraper.retry_after_cap_seconds)]

    def test_exponential_backoff_without_header(
        self, middleware: RetryAfterBackoffMiddleware, spider: DecisionsSpider, sleep_calls
    ) -> None:
        request = Request("https://www.workplacerelations.ie/en/search/", meta={"retry_times": 2})

        process(middleware, request, make_response(500), spider)

        base = get_settings().scraper.backoff_base_seconds
        # attempt 2 -> base * 2**2, with 0.5x-1.5x jitter
        assert base * 4 * 0.5 <= sleep_calls[0] <= base * 4 * 1.5

    def test_exhausted_retries_return_response_for_errback(
        self, middleware: RetryAfterBackoffMiddleware, spider: DecisionsSpider, sleep_calls
    ) -> None:
        request = Request("https://www.workplacerelations.ie/en/search/", meta={"retry_times": 2})

        result = process(middleware, request, make_response(500), spider)

        assert isinstance(result, Response)
        assert result.status == 500

    def test_dont_retry_meta_short_circuits(
        self, middleware: RetryAfterBackoffMiddleware, spider: DecisionsSpider, sleep_calls
    ) -> None:
        request = Request("https://www.workplacerelations.ie/en/search/", meta={"dont_retry": True})

        result = process(middleware, request, make_response(429), spider)

        assert isinstance(result, Response)
        assert sleep_calls == []


class TestCircuitBreaker:
    def test_consecutive_failures_trip_the_breaker(
        self,
        spider: DecisionsSpider,
        sleep_calls,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("WRC_SCRAPER__CIRCUIT_BREAKER_THRESHOLD", "3")
        get_settings.cache_clear()
        middleware = RetryAfterBackoffMiddleware.from_crawler(spider.crawler)
        request = Request("https://www.workplacerelations.ie/en/search/")

        process(middleware, request, make_response(503), spider)
        process(middleware, request, make_response(503), spider)
        with pytest.raises(CloseSpider):
            process(middleware, request, make_response(503), spider)


class TestParseRetryAfter:
    def test_delta_seconds(self) -> None:
        assert _parse_retry_after(b"30") == 30

    def test_http_date(self) -> None:
        future = datetime.now(tz=UTC) + timedelta(seconds=45)
        parsed = _parse_retry_after(format_datetime(future, usegmt=True))
        assert parsed is not None
        assert 40 <= parsed <= 45

    def test_past_http_date_clamps_to_zero(self) -> None:
        past = datetime.now(tz=UTC) - timedelta(seconds=45)
        assert _parse_retry_after(format_datetime(past, usegmt=True)) == 0

    def test_garbage_returns_none(self) -> None:
        assert _parse_retry_after(b"soon") is None
        assert _parse_retry_after(None) is None
