from wrc_pipeline.storage.s3 import ObjectStore


class TestObjectStore:
    def test_ensure_bucket_is_idempotent(self, s3_client) -> None:
        store = ObjectStore(s3_client)
        store.ensure_bucket("wrc-landing")
        store.ensure_bucket("wrc-landing")

        assert store.object_exists("wrc-landing", "missing") is False

    def test_put_get_roundtrip_preserves_bytes_and_content_type(self, s3_client) -> None:
        store = ObjectStore(s3_client)
        store.ensure_bucket("wrc-landing")
        payload = b"%PDF-1.4 \x00\x01 binary"

        store.put_bytes(
            "wrc-landing",
            "landing/body=1/partition=1999-12/EE47-1999.pdf",
            payload,
            content_type="application/pdf",
            metadata={"sha256": "e" * 64},
        )

        assert (
            store.get_bytes("wrc-landing", "landing/body=1/partition=1999-12/EE47-1999.pdf")
            == payload
        )
        head = s3_client.head_object(
            Bucket="wrc-landing", Key="landing/body=1/partition=1999-12/EE47-1999.pdf"
        )
        assert head["ContentType"] == "application/pdf"
        assert head["Metadata"]["sha256"] == "e" * 64

    def test_object_exists(self, s3_client) -> None:
        store = ObjectStore(s3_client)
        store.ensure_bucket("wrc-landing")
        store.put_bytes("wrc-landing", "k", b"data", content_type="text/html")

        assert store.object_exists("wrc-landing", "k") is True
        assert store.object_exists("wrc-landing", "other") is False
