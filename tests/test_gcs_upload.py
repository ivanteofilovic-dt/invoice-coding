"""Tests for historical GL and invoice upload to GCS (client mocked)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from invoice_processing.gcs_upload import (
    UploadSummary,
    _invoice_object_path,
    upload_historical_to_gcs,
)


@pytest.fixture
def mock_storage_client() -> MagicMock:
    client = MagicMock()
    bucket = MagicMock()
    client.bucket.return_value = bucket

    def blob_factory(name: str) -> MagicMock:
        b = MagicMock()
        b.name = name
        return b

    blobs: dict[str, MagicMock] = {}

    def blob(name: str) -> MagicMock:
        if name not in blobs:
            blobs[name] = blob_factory(name)
        return blobs[name]

    bucket.blob.side_effect = lambda n: blob(n)
    bucket._blobs = blobs  # type: ignore[attr-defined]
    return client


def test_upload_happy_path(tmp_path: Path, mock_storage_client: MagicMock) -> None:
    (tmp_path / "GL").mkdir()
    (tmp_path / "GL" / "a.txt").write_text("a", encoding="utf-8")
    (tmp_path / "GL" / "b.txt").write_text("b", encoding="utf-8")
    inv = tmp_path / "invoices" / "2026" / "q1"
    inv.mkdir(parents=True)
    (inv / "one.pdf").write_bytes(b"%PDF-1.4 minimal")
    (tmp_path / "invoices" / "root.pdf").write_bytes(b"%PDF-1.4 root")

    summary = upload_historical_to_gcs(
        "my-bucket", tmp_path, prefix="historical", client=mock_storage_client
    )

    assert summary.bucket == "my-bucket"
    assert summary.prefix == "historical"
    assert summary.gl_uploaded == 2
    assert summary.invoice_uploaded == 2
    assert summary.object_names == [
        "historical/gl/a.txt",
        "historical/gl/b.txt",
        "historical/invoices/2026/q1/one.pdf",
        "historical/invoices/root.pdf",
    ]

    bucket = mock_storage_client.bucket.return_value
    assert bucket.blob.call_count == 4
    for name in summary.object_names:
        bucket.blob.assert_any_call(name)

    all_blobs = bucket._blobs  # type: ignore[attr-defined]
    for obj_name in summary.object_names:
        b = all_blobs[obj_name]
        b.upload_from_filename.assert_called_once()
        args, _ = b.upload_from_filename.call_args
        local_path = Path(args[0])
        assert local_path.is_file()
        if obj_name.endswith(".pdf"):
            assert b.upload_from_filename.call_args[1]["content_type"] == "application/pdf"
        else:
            assert b.upload_from_filename.call_args[1]["content_type"] == "text/plain"


def test_gl_only_missing_invoices(tmp_path: Path, mock_storage_client: MagicMock) -> None:
    (tmp_path / "GL").mkdir()
    (tmp_path / "GL" / "only.txt").write_text("x", encoding="utf-8")

    summary = upload_historical_to_gcs("b", tmp_path, prefix="p", client=mock_storage_client)

    assert summary.gl_uploaded == 1
    assert summary.invoice_uploaded == 0
    assert summary.object_names == ["p/gl/only.txt"]


def test_invoices_only_missing_gl(tmp_path: Path, mock_storage_client: MagicMock) -> None:
    inv = tmp_path / "invoices"
    inv.mkdir()
    (inv / "doc.pdf").write_bytes(b"%PDF")

    summary = upload_historical_to_gcs("b", tmp_path, prefix="pfx", client=mock_storage_client)

    assert summary.gl_uploaded == 0
    assert summary.invoice_uploaded == 1
    assert summary.object_names == ["pfx/invoices/doc.pdf"]


def test_empty_tree(tmp_path: Path, mock_storage_client: MagicMock) -> None:
    summary = upload_historical_to_gcs("b", tmp_path, prefix="historical", client=mock_storage_client)

    assert summary == UploadSummary(bucket="b", prefix="historical", gl_uploaded=0, invoice_uploaded=0, object_names=[])
    mock_storage_client.bucket.return_value.blob.assert_not_called()


def test_prefix_strips_slashes(tmp_path: Path, mock_storage_client: MagicMock) -> None:
    (tmp_path / "GL").mkdir()
    (tmp_path / "GL" / "f.txt").write_text("z", encoding="utf-8")

    summary = upload_historical_to_gcs("b", tmp_path, prefix="  hist/  ", client=mock_storage_client)

    assert summary.prefix == "hist"
    assert summary.object_names == ["hist/gl/f.txt"]


def test_invoice_object_path_rejects_parent_segments() -> None:
    with pytest.raises(ValueError, match="Unsafe"):
        _invoice_object_path("historical", "a/../b.pdf")


def test_cli_requires_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    from invoice_processing import cli

    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
