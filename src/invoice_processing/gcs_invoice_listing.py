"""List invoice PDF objects in GCS as gs:// URIs (paginated)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud.storage import Client as StorageClient


def _normalize_prefix(prefix: str) -> str:
    p = prefix.strip().strip("/")
    return p


def list_invoice_pdf_uris(
    bucket: str,
    prefix: str,
    *,
    client: StorageClient | None = None,
    page_size: int = 1000,
) -> list[str]:
    """Return ``gs://`` URIs for blobs ending in ``.pdf`` under ``prefix``.

    ``prefix`` is treated like a logical directory (no leading slash; trailing
    slash optional). Listing is prefix-based and paginated via the Storage API.
    """
    from google.cloud.storage import Client as StorageClientImpl

    norm = _normalize_prefix(prefix)
    storage = client or StorageClientImpl()
    b = storage.bucket(bucket)
    out: list[str] = []
    for blob in b.list_blobs(prefix=norm, page_size=page_size):
        name = blob.name
        if not name.lower().endswith(".pdf"):
            continue
        out.append(f"gs://{bucket}/{name}")
    return out


def iter_invoice_pdf_uris(
    bucket: str,
    prefix: str,
    *,
    client: StorageClient | None = None,
    page_size: int = 1000,
) -> Iterator[str]:
    """Yield ``gs://`` URIs for PDF blobs (same rules as :func:`list_invoice_pdf_uris`)."""
    yield from list_invoice_pdf_uris(
        bucket, prefix, client=client, page_size=page_size
    )
