"""File hashing and change detection.

Two hashes per stored object:

- ``file_hash``   — sha256 of the exact bytes written to object storage
                    (integrity: the record always describes the stored object).
- ``content_hash`` — sha256 of a *canonicalized* byte stream, used to decide
                    whether a document actually changed between runs.

The distinction exists because the site's ASP.NET server appends a volatile
``<!-- Elapsed time: 0.0156199 -->`` comment to HTML pages, so two fetches of
an unchanged page differ byte-for-byte (verified empirically). Hashing raw
bytes would flag every document as changed on every run and silently defeat
idempotency. Binary files (PDF/DOC) are stable, so both hashes coincide there.
"""

import hashlib
import re

_VOLATILE_HTML_COMMENT = re.compile(
    rb"<!--\s*(?:Elapsed time|cached or not being)[^>]*?-->",
    re.IGNORECASE,
)
_WHITESPACE_RUNS = re.compile(rb"\s+")


def file_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def content_hash(data: bytes, *, is_html: bool) -> str:
    if not is_html:
        return file_hash(data)
    canonical = _VOLATILE_HTML_COMMENT.sub(b"", data)
    canonical = _WHITESPACE_RUNS.sub(b" ", canonical).strip()
    return hashlib.sha256(canonical).hexdigest()
