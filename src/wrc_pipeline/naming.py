"""Identifier sanitization and file-type detection.

Identifiers on the site range from clean ("ADJ-00054658") to filename-hostile
("IR - SC - 00002163", "UD237/2010 MN222/2010"). The original string stays the
business key in MongoDB; only file/object names use the sanitized form.
"""

import re

from wrc_pipeline.constants import (
    EXTENSION_BY_CONTENT_TYPE,
    MAGIC_OLE2,
    MAGIC_PDF,
    MAGIC_ZIP,
    Body,
)

_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_REPEATED_DASHES = re.compile(r"-{2,}")


def sanitize_identifier(identifier: str) -> str:
    """Turn an identifier into a safe, stable file stem.

    "IR - SC - 00002163" -> "IR-SC-00002163"; "UD237/2010c MN222/2010c" ->
    "UD237-2010c-MN222-2010c". Deterministic so re-runs produce the same key.
    """
    cleaned = _UNSAFE_CHARS.sub("-", identifier.strip())
    cleaned = _REPEATED_DASHES.sub("-", cleaned)
    return cleaned.strip("-.")


def detect_extension(url: str, content_type: str | None, body: bytes) -> str:
    """Pick a file extension: magic bytes beat the Content-Type header, which
    beats the URL — legacy government CMSes routinely get the first two wrong."""
    if body.startswith(MAGIC_PDF):
        return ".pdf"
    if body.startswith(MAGIC_OLE2):
        return ".doc"
    if body.startswith(MAGIC_ZIP) and not _looks_like_html(body):
        return ".docx"
    if content_type:
        base_type = content_type.split(";")[0].strip().lower()
        if base_type in EXTENSION_BY_CONTENT_TYPE:
            return EXTENSION_BY_CONTENT_TYPE[base_type]
    url_path = url.split("?")[0].lower()
    for ext in (".pdf", ".docx", ".doc", ".rtf", ".html"):
        if url_path.endswith(ext):
            return ext
    return ".html" if _looks_like_html(body) else ".bin"


def _looks_like_html(body: bytes) -> bool:
    head = body[:512].lstrip().lower()
    return head.startswith((b"<!doctype", b"<html"))


def landing_key(body: Body, partition_key: str, identifier: str, extension: str) -> str:
    """Hive-style landing-zone key, e.g.
    landing/body=15376/partition=2025-06/ADJ-00054658.html"""
    return f"landing/body={int(body)}/partition={partition_key}/{sanitize_identifier(identifier)}{extension}"


def attachment_key(
    body: Body, partition_key: str, identifier: str, index: int, extension: str
) -> str:
    stem = sanitize_identifier(identifier)
    return (
        f"landing/body={int(body)}/partition={partition_key}/{stem}__attachment_{index}{extension}"
    )


def curated_key(body: Body, partition_key: str, identifier: str, extension: str) -> str:
    """Curated-zone key; the file name itself is exactly `identifier.ext`."""
    return f"curated/body={int(body)}/partition={partition_key}/{sanitize_identifier(identifier)}{extension}"
