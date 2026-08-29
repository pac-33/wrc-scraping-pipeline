from tests.conftest import load_fixture
from wrc_pipeline.transform.extract import extract_relevant_content


class TestExtractOnRealCasePage:
    def test_keeps_decision_content(self) -> None:
        result = extract_relevant_content(
            load_fixture("case_page_adj00053864.html"), "ADJ-00053864"
        )
        html = result.html.decode("utf-8")

        assert "ADJUDICATION OFFICER DECISION" in html
        assert "Complainant" in html  # parties table survives
        assert result.content_char_count > 500
        assert result.content_is_empty is False
        assert result.used_fallback is False

    def test_strips_site_chrome(self) -> None:
        raw = load_fixture("case_page_adj00053864.html")
        result = extract_relevant_content(raw, "ADJ-00053864")
        html = result.html.decode("utf-8")

        # All present in the raw page, all boilerplate:
        assert "Cookie Policy" in raw.decode("utf-8", "ignore")
        assert "Cookie Policy" not in html
        assert "Advanced Search Filters" not in html
        assert "<script" not in html
        assert "Gaeilge" not in html  # language switcher

    def test_extracted_title_combines_identifier_and_heading(self) -> None:
        result = extract_relevant_content(
            load_fixture("case_page_adj00053864.html"), "ADJ-00053864"
        )

        assert result.title.startswith("ADJ-00053864")
        assert "ADJUDICATION OFFICER DECISION" in result.title

    def test_output_is_smaller_than_input(self) -> None:
        raw = load_fixture("case_page_adj00053864.html")
        result = extract_relevant_content(raw, "ADJ-00053864")

        assert len(result.html) < len(raw) / 2

    def test_legacy_stub_page_keeps_pdf_reference_text(self) -> None:
        result = extract_relevant_content(
            load_fixture("case_page_ee47_1999_with_pdf.html"), "EE47-1999"
        )
        html = result.html.decode("utf-8")

        assert "sexual harassment" in html  # the summary text of EE47-1999
        assert result.content_is_empty is False


class TestExtractEdgeCases:
    def test_missing_content_div_falls_back_to_body(self) -> None:
        page = b"<html><body><nav>menu</nav><p>Bare decision text without the usual container.</p></body></html>"
        result = extract_relevant_content(page, "X-1")

        assert result.used_fallback is True
        assert "Bare decision text" in result.html.decode()
        assert "menu" not in result.html.decode()

    def test_empty_content_is_flagged(self) -> None:
        page = b'<html><body><div class="content"><p>&nbsp;</p></div></body></html>'
        result = extract_relevant_content(page, "X-2")

        assert result.content_is_empty is True
        assert result.title == "X-2"

    def test_spacer_paragraphs_are_dropped(self) -> None:
        page = (
            b'<html><body><div class="content">'
            b"<p><b>&nbsp;</b></p><p>Real text</p><p>\xc2\xa0</p>"
            b"</div></body></html>"
        )
        result = extract_relevant_content(page, "X-3")
        html = result.html.decode("utf-8")

        assert "Real text" in html
        assert html.count("<p>") == 1
