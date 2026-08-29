"""Spider for the Decisions & Determinations search on workplacerelations.ie.

Crawl shape, per (monthly partition x body):

1. GET the search page (plain query params — the ASP.NET form's pagination
   links expose a GET API, so no __VIEWSTATE postback is needed).
2. Page 1 reports the total ("Shows 1 to 10 of N results"); pages 2..ceil(N/10)
   are scheduled immediately and fetched in parallel under AutoThrottle.
3. Every result row yields a follow-up request: the "View Page" target is
   either an HTML case page (stored as the document) or, rarely, a direct
   PDF/DOC link (stored as-is). Case pages of legacy decisions additionally
   link the original PDF/DOC — downloaded as attachments.

Every request carries an errback, so any record the site reported but we could
not store ends up in the failure log with its reason — the run summary then
reconciles found vs. scraped vs. skipped-unchanged vs. failed.
"""

import math
import re
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from typing import Any, ClassVar
from urllib.parse import urlencode
from uuid import uuid4

import scrapy
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Response, TextResponse
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError, TimeoutError
from twisted.python.failure import Failure

from wrc_pipeline.config import ScraperSettings, get_settings
from wrc_pipeline.constants import (
    ALLOWED_DOMAIN,
    BASE_URL,
    BODY_LABELS,
    DOCUMENT_FILE_EXTENSIONS,
    RESULTS_PER_PAGE,
    SEARCH_PARAM_BODY,
    SEARCH_PARAM_DECISIONS,
    SEARCH_PARAM_FROM,
    SEARCH_PARAM_PAGE,
    SEARCH_PARAM_TO,
    SEARCH_PATH,
    SITE_DATE_FORMAT,
    Body,
)
from wrc_pipeline.models import DocKind, utcnow
from wrc_pipeline.scraping.items import AttachmentItem, DocumentItem
from wrc_pipeline.scraping.partitions import Partition, month_partitions

_RESULTS_COUNTER = re.compile(r"Shows\s+[\d,]+\s+to\s+[\d,]+\s+of\s+([\d,]+)\s+results")

# Paths robots.txt disallows but which host the canonical decision documents
# linked from (allowed) case pages — see ScraperSettings.fetch_robots_disallowed_documents.
_IMPORT_PATH_MARKER = "_import/"


class DecisionsSpider(scrapy.Spider):
    name = "decisions"
    allowed_domains: ClassVar[list[str]] = [ALLOWED_DOMAIN]

    def __init__(
        self,
        start_date: str,
        end_date: str,
        bodies: str | None = None,
        run_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.scraper_settings: ScraperSettings = get_settings().scraper
        self.partitions = month_partitions(_parse_date(start_date), _parse_date(end_date))
        self.bodies = _parse_bodies(bodies) or self.scraper_settings.bodies
        self.run_id = run_id or uuid4().hex
        self.started_at = utcnow()
        self.start_date_arg = start_date
        self.end_date_arg = end_date
        # (partition_key, body) -> total the site reported; reconciled at close.
        self.expected_counts: dict[tuple[str, int], int] = {}
        self.failures: list[dict[str, Any]] = []

    async def start(self) -> AsyncIterator[scrapy.Request]:
        self.logger.info(
            "crawl_planned",
            extra={
                "run_id": self.run_id,
                "partitions": [p.key for p in self.partitions],
                "bodies": [BODY_LABELS[b] for b in self.bodies],
            },
        )
        for partition in self.partitions:
            for body in self.bodies:
                yield self._search_request(partition, body, page=1)

    def _search_request(self, partition: Partition, body: Body, page: int) -> scrapy.Request:
        params = {
            SEARCH_PARAM_DECISIONS: 1,
            SEARCH_PARAM_FROM: partition.query_from,
            SEARCH_PARAM_TO: partition.query_to,
            SEARCH_PARAM_BODY: int(body),
            SEARCH_PARAM_PAGE: page,
        }
        return scrapy.Request(
            f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}",
            callback=self.parse_search_page,  # type: ignore[arg-type]
            errback=self.handle_request_failure,
            cb_kwargs={"partition": partition, "body": body, "page": page},
        )

    def parse_search_page(
        self, response: TextResponse, partition: Partition, body: Body, page: int
    ) -> Iterator[scrapy.Request]:
        rows = response.css("li.each-item")
        total = self._extract_total(response)

        if total is None:
            if not rows and response.css("form#form"):
                # A no-result search renders no counter at all — a legitimate
                # empty partition, only trustworthy because the form is present.
                self.crawler.stats.inc_value("wrc/partitions_empty")
                self._log_partition(partition, body, "partition_empty")
                return
            self._record_failure(
                reason="unexpected_page_structure",
                url=response.url,
                partition=partition.key,
                body=body,
                detail="results counter and search form both missing",
            )
            return

        if page == 1:
            self.expected_counts[(partition.key, int(body))] = total
            self.crawler.stats.inc_value("wrc/records_found", total)
            self._log_partition(partition, body, "partition_search_started", total=total)
            last_page = math.ceil(total / RESULTS_PER_PAGE)
            for later_page in range(2, last_page + 1):
                yield self._search_request(partition, body, later_page)

        for row in rows:
            request = self._record_request(row, response, partition, body)
            if request is not None:
                yield request

    def _record_request(
        self, row: Any, response: TextResponse, partition: Partition, body: Body
    ) -> scrapy.Request | None:
        identifier = (row.css("h2.title::attr(title)").get() or "").strip()
        href = row.css("div.link a::attr(href)").get() or row.css("h2.title a::attr(href)").get()
        if not identifier or not href:
            self._record_failure(
                reason="record_parse_error",
                url=response.url,
                partition=partition.key,
                body=body,
                detail=f"identifier={identifier!r} href={href!r}",
            )
            return None

        record = {
            "identifier": identifier,
            "title": identifier,
            "description": _clean_description(row.css("p.description::attr(title)").get()),
            "published_date": _parse_site_date(row.css("span.date::text").get()),
            "partition_key": partition.key,
            "partition_date": partition.partition_date,
            "body": body,
            "source_page_url": response.url,
        }
        doc_url = response.urljoin(href)
        if _is_document_file(doc_url):
            return scrapy.Request(
                doc_url,
                callback=self.parse_direct_file,
                errback=self.handle_request_failure,
                cb_kwargs={"record": record},
                meta=self._document_request_meta(doc_url),
            )
        return scrapy.Request(
            doc_url,
            callback=self.parse_case_page,  # type: ignore[arg-type]
            errback=self.handle_request_failure,
            cb_kwargs={"record": record},
        )

    def parse_case_page(
        self, response: TextResponse, record: dict[str, Any]
    ) -> Iterator[DocumentItem | scrapy.Request]:
        yield DocumentItem(
            **record,
            doc_url=response.url,
            doc_kind=DocKind.HTML_PAGE,
            raw_body=bytes(response.body),
            content_type_header=_content_type(response),
        )
        if not self.scraper_settings.download_attachments:
            return
        attachment_urls = [
            response.urljoin(href)
            for href in response.css("div.content a::attr(href)").getall()
            if _is_document_file(response.urljoin(href))
        ]
        for index, url in enumerate(attachment_urls, start=1):
            yield scrapy.Request(
                url,
                callback=self.parse_attachment,
                errback=self.handle_request_failure,
                cb_kwargs={
                    "identifier": record["identifier"],
                    "body": record["body"],
                    "partition_key": record["partition_key"],
                    "index": index,
                },
                meta=self._document_request_meta(url),
            )

    def parse_direct_file(
        self, response: Response, record: dict[str, Any]
    ) -> Iterator[DocumentItem]:
        yield DocumentItem(
            **record,
            doc_url=response.url,
            doc_kind=DocKind.FILE,
            raw_body=bytes(response.body),
            content_type_header=_content_type(response),
        )

    def parse_attachment(
        self, response: Response, identifier: str, body: Body, partition_key: str, index: int
    ) -> Iterator[AttachmentItem]:
        yield AttachmentItem(
            identifier=identifier,
            body=body,
            partition_key=partition_key,
            url=response.url,
            index=index,
            raw_body=bytes(response.body),
            content_type_header=_content_type(response),
        )

    def handle_request_failure(self, failure: Failure) -> None:
        request: scrapy.Request = failure.request  # type: ignore[attr-defined]
        context = dict(request.cb_kwargs)
        record = context.pop("record", None) or {}
        detail: dict[str, Any] = {
            "url": request.url,
            "identifier": record.get("identifier") or context.get("identifier"),
            "partition": record.get("partition_key")
            or context.get("partition_key")
            or getattr(context.get("partition"), "key", None),
            "retry_times": request.meta.get("retry_times", 0),
        }
        if failure.check(HttpError):  # type: ignore[no-untyped-call]
            detail["reason"] = "http_error"
            detail["http_status"] = failure.value.response.status  # type: ignore[union-attr]
        elif failure.check(DNSLookupError):  # type: ignore[no-untyped-call]
            detail["reason"] = "dns_lookup_error"
        elif failure.check(TimeoutError, TCPTimedOutError):  # type: ignore[no-untyped-call]
            detail["reason"] = "timeout"
        elif failure.check(IgnoreRequest):  # type: ignore[no-untyped-call]
            detail["reason"] = "forbidden_by_robotstxt"
        else:
            detail["reason"] = "download_error"
            detail["detail"] = repr(failure.value)
        self._record_failure(**detail)

    def _document_request_meta(self, url: str) -> dict[str, Any]:
        """Robots exception for document files under the legacy /..._Import/
        paths: robots.txt disallows crawling them, but the (allowed) case pages
        link them as the canonical decision documents. Config-gated and logged;
        with the flag off, the robots middleware drops the request and the
        errback records it as forbidden_by_robotstxt."""
        if (
            self.scraper_settings.fetch_robots_disallowed_documents
            and _IMPORT_PATH_MARKER in url.lower()
        ):
            self.crawler.stats.inc_value("wrc/robots_exception_downloads")
            return {"dont_obey_robotstxt": True}
        return {}

    def _extract_total(self, response: TextResponse) -> int | None:
        match = _RESULTS_COUNTER.search(response.text)
        return int(match.group(1).replace(",", "")) if match else None

    def _log_partition(self, partition: Partition, body: Body, event: str, **fields: Any) -> None:
        self.logger.info(
            event,
            extra={
                "run_id": self.run_id,
                "partition": partition.key,
                "body": BODY_LABELS[body],
                **fields,
            },
        )

    def _record_failure(self, **detail: Any) -> None:
        body = detail.get("body")
        if isinstance(body, Body):
            detail["body"] = BODY_LABELS[body]
        entry = {key: value for key, value in detail.items() if value is not None}
        self.failures.append(entry)
        self.crawler.stats.inc_value("wrc/failures")
        self.crawler.stats.inc_value(f"wrc/failures/{entry.get('reason', 'unknown')}")
        self.logger.error("record_failed", extra={"run_id": self.run_id, **entry})


def _parse_date(value: str) -> date:
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC).date()
        except ValueError:
            continue
    msg = f"invalid date {value!r}: expected YYYY-MM-DD or YYYY-MM"
    raise ValueError(msg)


def _parse_bodies(value: str | None) -> list[Body] | None:
    if not value:
        return None
    return [Body(int(part)) for part in value.split(",") if part.strip()]


def _parse_site_date(value: str | None) -> datetime | None:
    if not value or not value.strip():
        return None
    try:
        return datetime.strptime(value.strip(), SITE_DATE_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None


def _clean_description(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned or None


def _content_type(response: Response) -> str | None:
    header = response.headers.get("Content-Type")
    return header.decode("latin-1") if header else None


def _is_document_file(url: str) -> bool:
    path = url.split("?")[0].lower()
    return any(path.endswith(ext) for ext in DOCUMENT_FILE_EXTENSIONS)
