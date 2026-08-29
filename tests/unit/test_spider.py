from datetime import UTC, datetime

import pytest
import scrapy
from scrapy.http import HtmlResponse, Request
from scrapy.utils.test import get_crawler

from tests.conftest import load_fixture
from wrc_pipeline.constants import Body
from wrc_pipeline.models import DocKind
from wrc_pipeline.scraping.items import DocumentItem
from wrc_pipeline.scraping.partitions import month_partitions
from wrc_pipeline.scraping.spiders.decisions import DecisionsSpider, _parse_date

SEARCH_URL = (
    "https://www.workplacerelations.ie/en/search/"
    "?decisions=1&from=01%2F06%2F2025&to=30%2F06%2F2025&body=15376&pageNumber=1"
)


@pytest.fixture
def spider() -> DecisionsSpider:
    crawler = get_crawler(DecisionsSpider)
    return DecisionsSpider.from_crawler(crawler, start_date="2025-06-01", end_date="2025-06-30")


def search_response(body: bytes, url: str = SEARCH_URL) -> HtmlResponse:
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=Request(url))


def run_parse_search(spider: DecisionsSpider, response: HtmlResponse) -> list[Request]:
    (partition,) = spider.partitions
    return list(
        spider.parse_search_page(
            response, partition=partition, body=Body.WORKPLACE_RELATIONS_COMMISSION, page=1
        )
    )


class TestParseSearchPage:
    def test_page_one_fans_out_remaining_pages_and_follows_records(
        self, spider: DecisionsSpider
    ) -> None:
        """WRC June 2025 fixture reports 192 results -> 20 pages total, so page
        1 must schedule pages 2..20 (19 requests) plus 10 case-page requests."""
        outputs = run_parse_search(
            spider, search_response(load_fixture("search_results_wrc_june2025_p1.html"))
        )

        search_requests = [r for r in outputs if r.callback == spider.parse_search_page]
        case_requests = [r for r in outputs if r.callback == spider.parse_case_page]
        assert len(search_requests) == 19
        assert len(case_requests) == 10
        assert spider.expected_counts[("2025-06", int(Body.WORKPLACE_RELATIONS_COMMISSION))] == 192
        assert spider.crawler.stats.get_value("wrc/records_found") == 192

    def test_record_requests_carry_full_metadata(self, spider: DecisionsSpider) -> None:
        outputs = run_parse_search(
            spider, search_response(load_fixture("search_results_wrc_june2025_p1.html"))
        )

        record = next(r for r in outputs if r.callback == spider.parse_case_page).cb_kwargs[
            "record"
        ]
        assert record["identifier"]
        assert record["partition_key"] == "2025-06"
        assert record["partition_date"] == datetime(2025, 6, 1, tzinfo=UTC)
        assert record["body"] == Body.WORKPLACE_RELATIONS_COMMISSION
        assert record["published_date"] is not None
        assert record["published_date"].tzinfo is not None
        assert record["title"] == record["identifier"]
        assert record["source_page_url"] == SEARCH_URL

    def test_every_request_has_an_errback(self, spider: DecisionsSpider) -> None:
        outputs = run_parse_search(
            spider, search_response(load_fixture("search_results_wrc_june2025_p1.html"))
        )

        assert outputs
        assert all(request.errback is not None for request in outputs)

    def test_empty_partition_is_not_an_error(self, spider: DecisionsSpider) -> None:
        """A no-result search renders no counter at all; the search form being
        present is what distinguishes 'empty' from 'broken page'."""
        outputs = run_parse_search(
            spider, search_response(load_fixture("search_results_empty.html"))
        )

        assert outputs == []
        assert spider.failures == []
        assert spider.crawler.stats.get_value("wrc/partitions_empty") == 1

    def test_unexpected_page_structure_is_recorded_as_failure(
        self, spider: DecisionsSpider
    ) -> None:
        outputs = run_parse_search(
            spider, search_response(b"<html><body>Service unavailable</body></html>")
        )

        assert outputs == []
        assert len(spider.failures) == 1
        assert spider.failures[0]["reason"] == "unexpected_page_structure"
        assert spider.crawler.stats.get_value("wrc/failures") == 1


class TestParseCasePage:
    def make_record(self, spider: DecisionsSpider) -> dict:
        (partition,) = spider.partitions
        return {
            "identifier": "ADJ-00053864",
            "title": "ADJ-00053864",
            "description": "Sandra Olszewska v Ospg Ennis",
            "published_date": datetime(2025, 6, 10, tzinfo=UTC),
            "partition_key": partition.key,
            "partition_date": partition.partition_date,
            "body": Body.WORKPLACE_RELATIONS_COMMISSION,
            "source_page_url": SEARCH_URL,
        }

    def test_case_page_yields_document_item(self, spider: DecisionsSpider) -> None:
        url = "https://www.workplacerelations.ie/en/cases/2025/june/adj-00053864.html"
        response = HtmlResponse(
            url=url,
            body=load_fixture("case_page_adj00053864.html"),
            encoding="utf-8",
            request=Request(url),
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

        outputs = list(spider.parse_case_page(response, self.make_record(spider)))

        assert len(outputs) == 1
        item = outputs[0]
        assert isinstance(item, DocumentItem)
        assert item.doc_kind == DocKind.HTML_PAGE
        assert item.raw_body == load_fixture("case_page_adj00053864.html")
        assert item.content_type_header == "text/html; charset=utf-8"

    def test_legacy_stub_page_also_requests_pdf_attachment(self, spider: DecisionsSpider) -> None:
        """EE47-1999's case page is a thin stub whose real decision is a linked
        PDF under the robots-disallowed /en/Equality_Tribunal_Import/ path."""
        url = "https://www.workplacerelations.ie/en/cases/1999/december/ee47-1999.html"
        record = self.make_record(spider) | {"identifier": "EE47-1999"}
        response = HtmlResponse(
            url=url,
            body=load_fixture("case_page_ee47_1999_with_pdf.html"),
            encoding="utf-8",
            request=Request(url),
        )

        outputs = list(spider.parse_case_page(response, record))

        items = [o for o in outputs if isinstance(o, DocumentItem)]
        attachment_requests = [o for o in outputs if isinstance(o, Request)]
        assert len(items) == 1
        assert len(attachment_requests) == 1
        attachment = attachment_requests[0]
        assert attachment.url.lower().endswith(".pdf")
        assert "_import/" in attachment.url.lower()
        assert attachment.meta["dont_obey_robotstxt"] is True
        assert attachment.cb_kwargs["identifier"] == "EE47-1999"
        assert attachment.cb_kwargs["index"] == 1
        assert spider.crawler.stats.get_value("wrc/robots_exception_downloads") == 1


class TestParseDirectFile:
    def test_direct_file_yields_binary_document_item(self, spider: DecisionsSpider) -> None:
        url = "https://www.workplacerelations.ie/en/some/decision.pdf"
        pdf_bytes = b"%PDF-1.4 direct file"
        response = scrapy.http.Response(
            url=url,
            body=pdf_bytes,
            request=Request(url),
            headers={"Content-Type": "application/pdf"},
        )
        (partition,) = spider.partitions
        record = {
            "identifier": "SOME-123",
            "title": "SOME-123",
            "description": None,
            "published_date": None,
            "partition_key": partition.key,
            "partition_date": partition.partition_date,
            "body": Body.LABOUR_COURT,
            "source_page_url": SEARCH_URL,
        }

        (item,) = list(spider.parse_direct_file(response, record))

        assert item.doc_kind == DocKind.FILE
        assert item.raw_body == pdf_bytes
        assert item.content_type_header == "application/pdf"


class TestSpiderArguments:
    def test_accepts_year_month_shorthand(self) -> None:
        crawler = get_crawler(DecisionsSpider)
        spider = DecisionsSpider.from_crawler(crawler, start_date="2025-01", end_date="2025-03")

        assert [p.key for p in spider.partitions] == ["2025-01", "2025-02", "2025-03"]

    def test_invalid_date_fails_fast(self) -> None:
        with pytest.raises(ValueError, match="invalid date"):
            _parse_date("01/06/2025")

    def test_bodies_argument_filters_bodies(self) -> None:
        crawler = get_crawler(DecisionsSpider)
        spider = DecisionsSpider.from_crawler(
            crawler, start_date="2025-06", end_date="2025-06", bodies="3,15376"
        )

        assert spider.bodies == [Body.LABOUR_COURT, Body.WORKPLACE_RELATIONS_COMMISSION]

    def test_partitions_cover_requested_range(self) -> None:
        assert len(month_partitions(_parse_date("2024-01-01"), _parse_date("2024-06-30"))) == 6
