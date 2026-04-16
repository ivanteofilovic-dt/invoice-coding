"""Upload historical GL exports and invoice PDFs to Google Cloud Storage."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud.storage import Client as StorageClient


@dataclass
class UploadSummary:
    """Result of a historical data upload run."""

    bucket: str
    prefix: str
    gl_uploaded: int = 0
    invoice_uploaded: int = 0
    object_names: list[str] = field(default_factory=list)


def _normalize_prefix(prefix: str) -> str:
    return prefix.strip().strip("/")


def _gl_object_path(prefix: str, filename: str) -> str:
    base = _normalize_prefix(prefix)
    if base:
        return f"{base}/gl/{filename}"
    return f"gl/{filename}"


def _invoice_object_path(prefix: str, relative_pdf: str) -> str:
    rel = relative_pdf.replace("\\", "/").lstrip("/")
    if ".." in rel.split("/"):
        msg = f"Unsafe invoice object path: {rel!r}"
        raise ValueError(msg)
    base = _normalize_prefix(prefix)
    if base:
        return f"{base}/invoices/{rel}"
    return f"invoices/{rel}"


def _collect_gl_files(gl_dir: Path) -> list[Path]:
    if not gl_dir.is_dir():
        return []
    return sorted(p for p in gl_dir.glob("*.txt") if p.is_file())


def _collect_invoice_files(invoices_dir: Path) -> list[tuple[Path, str]]:
    """Return (absolute_path, relative_posix_path) for each PDF under invoices_dir."""
    if not invoices_dir.is_dir():
        return []
    root = invoices_dir.resolve()
    out: list[tuple[Path, str]] = []
    for path in sorted(invoices_dir.rglob("*.pdf")):
        if not path.is_file():
            continue
        resolved = path.resolve()
        try:
            resolved.relative_to(root)
        except ValueError:
            continue
        if not resolved.is_relative_to(root):
            continue
        rel = resolved.relative_to(root).as_posix()
        if ".." in Path(rel).parts:
            continue
        out.append((resolved, rel))
    return out


def upload_historical_to_gcs(
    bucket: str,
    data_root: Path,
    *,
    prefix: str = "historical",
    client: StorageClient | None = None,
) -> UploadSummary:
    """Upload GL ``*.txt`` files and invoice ``*.pdf`` files under ``data_root`` to GCS.

    - Reads ``data_root / "GL"`` for ``*.txt`` (non-recursive).
    - Reads ``data_root / "invoices"`` for ``*.pdf`` (recursive); object keys preserve
      relative paths under ``invoices/``.
    - Missing ``GL`` or ``invoices`` directories are treated as empty.

    Object layout:

    - ``{prefix}/gl/{filename}.txt``
    - ``{prefix}/invoices/{relative/path}.pdf``

    Args:
        bucket: GCS bucket name.
        data_root: Root directory containing ``GL`` and ``invoices`` subfolders.
        prefix: Key prefix inside the bucket (leading/trailing slashes stripped).
        client: Optional pre-configured ``google.cloud.storage.Client`` (for tests).

    Returns:
        UploadSummary with counts and ordered object names.
    """
    from google.cloud.storage import Client as StorageClientImpl

    data_root = data_root.resolve()
    gl_dir = data_root / "GL"
    invoices_dir = data_root / "invoices"
    norm_prefix = _normalize_prefix(prefix)

    storage_client = client or StorageClientImpl()
    bucket_ref = storage_client.bucket(bucket)
    summary = UploadSummary(bucket=bucket, prefix=norm_prefix)

    for gl_path in _collect_gl_files(gl_dir):
        name = gl_path.name
        object_name = _gl_object_path(norm_prefix, name)
        blob = bucket_ref.blob(object_name)
        blob.upload_from_filename(str(gl_path), content_type="text/plain")
        summary.gl_uploaded += 1
        summary.object_names.append(object_name)

    for inv_path, rel in _collect_invoice_files(invoices_dir):
        object_name = _invoice_object_path(norm_prefix, rel)
        blob = bucket_ref.blob(object_name)
        blob.upload_from_filename(str(inv_path), content_type="application/pdf")
        summary.invoice_uploaded += 1
        summary.object_names.append(object_name)

    return summary
