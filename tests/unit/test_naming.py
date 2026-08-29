import pytest

from wrc_pipeline.constants import MAGIC_OLE2, Body
from wrc_pipeline.naming import (
    attachment_key,
    curated_key,
    detect_extension,
    landing_key,
    sanitize_identifier,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("ADJ-00054658", "ADJ-00054658"),
        ("IR - SC - 00002163", "IR-SC-00002163"),
        ("UD237/2010c MN222/2010c", "UD237-2010c-MN222-2010c"),
        ("  LCR23157  ", "LCR23157"),
        ("DEC-E2008-013 - Full Case Report", "DEC-E2008-013-Full-Case-Report"),
    ],
)
def test_sanitize_identifier(raw: str, expected: str) -> None:
    assert sanitize_identifier(raw) == expected


def test_sanitize_is_deterministic() -> None:
    assert sanitize_identifier("IR - SC - 1") == sanitize_identifier("IR - SC - 1")


class TestDetectExtension:
    def test_magic_bytes_beat_lying_content_type(self) -> None:
        pdf_body = b"%PDF-1.4 rest of file"
        assert detect_extension("https://x/file.ashx", "text/html", pdf_body) == ".pdf"

    def test_legacy_doc_signature(self) -> None:
        assert detect_extension("https://x/d", None, MAGIC_OLE2 + b"rest") == ".doc"

    def test_docx_zip_signature(self) -> None:
        assert detect_extension("https://x/d", None, b"PK\x03\x04rest") == ".docx"

    def test_content_type_when_no_magic_match(self) -> None:
        assert detect_extension("https://x/d", "application/pdf; charset=x", b"garbled") == ".pdf"

    def test_url_extension_as_fallback(self) -> None:
        assert detect_extension("https://x/report.DOC?v=1", None, b"unknown") == ".doc"

    def test_html_sniffing_as_last_resort(self) -> None:
        assert detect_extension("https://x/page", None, b"  <!DOCTYPE html><html>") == ".html"

    def test_unknown_binary(self) -> None:
        assert detect_extension("https://x/blob", None, b"\x00\x01\x02") == ".bin"


def test_landing_key_layout() -> None:
    key = landing_key(Body.WORKPLACE_RELATIONS_COMMISSION, "2025-06", "ADJ-00054658", ".html")
    assert key == "landing/body=15376/partition=2025-06/ADJ-00054658.html"


def test_attachment_key_layout() -> None:
    key = attachment_key(Body.EQUALITY_TRIBUNAL, "1999-12", "EE47-1999", 1, ".pdf")
    assert key == "landing/body=1/partition=1999-12/EE47-1999__attachment_1.pdf"


def test_curated_key_uses_sanitized_identifier() -> None:
    key = curated_key(Body.LABOUR_COURT, "2025-06", "IR - SC - 00002163", ".html")
    assert key == "curated/body=3/partition=2025-06/IR-SC-00002163.html"
