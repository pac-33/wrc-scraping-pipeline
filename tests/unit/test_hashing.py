from wrc_pipeline.hashing import content_hash, file_hash

PAGE_FETCH_1 = b'<html><body><div class="content">Decision text</div></body></html>\n<!-- cached or not being index.aspx page --><!-- Elapsed time: 0.0156199 -->'
PAGE_FETCH_2 = b'<html><body><div class="content">Decision text</div></body></html>\n<!-- cached or not being index.aspx page --><!-- Elapsed time: 0 -->'
PAGE_CHANGED = b'<html><body><div class="content">Amended decision text</div></body></html>\n<!-- Elapsed time: 3 -->'


def test_volatile_server_comment_does_not_change_content_hash() -> None:
    """Two fetches of the same page differ only by the ASP.NET timing comment
    (verified against the live site) — content_hash must treat them as equal."""
    assert file_hash(PAGE_FETCH_1) != file_hash(PAGE_FETCH_2)
    assert content_hash(PAGE_FETCH_1, is_html=True) == content_hash(PAGE_FETCH_2, is_html=True)


def test_real_content_change_is_detected() -> None:
    assert content_hash(PAGE_FETCH_1, is_html=True) != content_hash(PAGE_CHANGED, is_html=True)


def test_whitespace_reflow_does_not_change_content_hash() -> None:
    a = b"<html><body>Decision   text</body></html>"
    b = b"<html><body>Decision\n  text</body></html>"
    assert content_hash(a, is_html=True) == content_hash(b, is_html=True)


def test_binary_content_hash_equals_file_hash() -> None:
    pdf = b"%PDF-1.4 binary bytes \x00\x01"
    assert content_hash(pdf, is_html=False) == file_hash(pdf)


def test_file_hash_is_sha256_hex() -> None:
    digest = file_hash(b"abc")
    assert digest == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
