"""Site facts and shared constants for the workplacerelations.ie pipeline.

Values here describe the target website itself (verified against the live site)
and are not tunable configuration — tunables live in `wrc_pipeline.config`.
"""

from enum import IntEnum

BASE_URL = "https://www.workplacerelations.ie"
SEARCH_PATH = "/en/search/"
ALLOWED_DOMAIN = "www.workplacerelations.ie"

# The search endpoint accepts plain GET query parameters (discovered via the
# pagination links the ASP.NET page renders — no __VIEWSTATE postback needed).
SEARCH_PARAM_DECISIONS = "decisions"
SEARCH_PARAM_FROM = "from"
SEARCH_PARAM_TO = "to"
SEARCH_PARAM_BODY = "body"
SEARCH_PARAM_PAGE = "pageNumber"

# Dates in search params and result listings are rendered as e.g. 17/07/2025.
SITE_DATE_FORMAT = "%d/%m/%Y"

# The site returns a fixed page size; the pageSize parameter is ignored.
RESULTS_PER_PAGE = 10


class Body(IntEnum):
    """Adjudicating body ids as used by the search form's Body filter."""

    EQUALITY_TRIBUNAL = 1
    EMPLOYMENT_APPEALS_TRIBUNAL = 2
    LABOUR_COURT = 3
    WORKPLACE_RELATIONS_COMMISSION = 15376


BODY_LABELS: dict[Body, str] = {
    Body.EQUALITY_TRIBUNAL: "Equality Tribunal",
    Body.EMPLOYMENT_APPEALS_TRIBUNAL: "Employment Appeals Tribunal",
    Body.LABOUR_COURT: "Labour Court",
    Body.WORKPLACE_RELATIONS_COMMISSION: "Workplace Relations Commission",
}

# File extensions treated as directly downloadable documents (spec 6a);
# everything else reached via a "View Page" link is an HTML case page (spec 6b).
DOCUMENT_FILE_EXTENSIONS = frozenset({".pdf", ".doc", ".docx", ".rtf"})

EXTENSION_BY_CONTENT_TYPE = {
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/rtf": ".rtf",
    "text/html": ".html",
}

# Magic-byte signatures — ground truth when URLs and Content-Type headers lie.
MAGIC_PDF = b"%PDF-"
MAGIC_OLE2 = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"  # legacy .doc container
MAGIC_ZIP = b"PK\x03\x04"  # .docx container
