"""Relevant-content extraction from case-page HTML.

A case page's decision text lives in ``div.content`` (verified across WRC,
Labour Court, EAT and Equality Tribunal pages back to 1999). Everything around
it — navigation, cookie banner, search widgets, header, footer, scripts — is
boilerplate the transformation must drop. The extracted fragment is re-wrapped
as a minimal standalone HTML document.
"""

from dataclasses import dataclass

from bs4 import BeautifulSoup, Tag

_CONTENT_SELECTOR = "div.content"
_NOISE_TAGS = ("script", "style", "noscript", "iframe", "nav", "header", "footer", "form")
# Text shorter than this almost certainly means extraction grabbed the wrong
# node or the source page is a stub — flagged for data-quality review.
_MIN_CONTENT_CHARS = 40

_DOCUMENT_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{title}</title>
</head>
<body>
{body}
</body>
</html>
"""


@dataclass(frozen=True)
class ExtractionResult:
    html: bytes
    title: str
    content_char_count: int
    content_is_empty: bool
    used_fallback: bool


def extract_relevant_content(raw_html: bytes, identifier: str) -> ExtractionResult:
    soup = BeautifulSoup(raw_html, "lxml")
    content = soup.select_one(_CONTENT_SELECTOR)
    used_fallback = content is None
    if content is None:
        # Structure drift / very old pages: fall back to <body> minus chrome
        # rather than losing the record, and flag it.
        content = soup.body or soup

    for tag in content.find_all(_NOISE_TAGS):
        tag.decompose()
    _drop_empty_paragraphs(content)

    text = content.get_text(separator=" ", strip=True)
    title = _extract_title(content, identifier)
    rendered = _DOCUMENT_TEMPLATE.format(title=title, body=content.decode_contents().strip())
    return ExtractionResult(
        html=rendered.encode("utf-8"),
        title=title,
        content_char_count=len(text),
        content_is_empty=len(text) < _MIN_CONTENT_CHARS,
        used_fallback=used_fallback,
    )


def _extract_title(content: Tag, identifier: str) -> str:
    heading = content.find(["h1", "h2"])
    if heading is not None:
        text = heading.get_text(strip=True)
        if text:
            return f"{identifier} — {text}"
    return identifier


def _drop_empty_paragraphs(content: Tag) -> None:
    """Case pages are littered with <p><b>&nbsp;</b></p> spacer paragraphs."""
    for paragraph in content.find_all("p"):
        if not paragraph.get_text(strip=True).replace("\xa0", ""):
            paragraph.decompose()
