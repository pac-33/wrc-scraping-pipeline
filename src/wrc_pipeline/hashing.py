"""File hashing and change detection: one sha256 per stored object.

``file_hash`` is the digest of the exact bytes for binary documents (PDF/DOC).
For HTML it is the digest of a *canonicalised* byte stream: the site's ASP.NET
server appends a volatile ``<!-- Elapsed time: 0.0156199 -->`` comment to every
page, so two fetches of an unchanged page differ byte-for-byte (verified
empirically). Hashing raw bytes would flag every document as changed on every
run and silently defeat idempotency. Byte-level integrity of what sits in the
bucket is the object store's job (ETag / upload checksums), not a second
application-level hash.
"""

import hashlib
import re

_VOLATILE_HTML_COMMENT = re.compile(
    rb"<!--\s*(?:Elapsed time|cached or not being)[^>]*?-->",
    re.IGNORECASE,
)
_WHITESPACE_RUNS = re.compile(rb"\s+")


def file_hash(data: bytes, *, is_html: bool = False) -> str:
    """sha256 hex digest; HTML is canonicalised first so re-fetches compare equal."""
    if not is_html:
        return hashlib.sha256(data).hexdigest()
    canonical = _VOLATILE_HTML_COMMENT.sub(b"", data)
    canonical = _WHITESPACE_RUNS.sub(b" ", canonical).strip()
    return hashlib.sha256(canonical).hexdigest()
