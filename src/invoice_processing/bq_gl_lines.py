"""BigQuery schema, filtering, and load for GL export TSV files."""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

from google.cloud import bigquery

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from google.cloud.bigquery import Client as BigQueryClient

# Header row from GL export (SUPPLIER_CUSTMER_NAME spelling matches source).
GL_EXPORT_COLUMNS: tuple[str, ...] = (
    "ENTITY",
    "GL_SOURCE_NAME",
    "GL_CATEGORY",
    "JOURNAL_NUMBER",
    "BOOKING_DATE",
    "PERIOD",
    "ACCOUNT",
    "HFM_ACCOUNT",
    "HFM_DSCRIPTIONS",
    "DEPARTMENT",
    "PRODUCT",
    "WORK_ORDER",
    "IC",
    "PROJECT",
    "SYSTEM",
    "RESERVE",
    "INVOICE_NUM",
    "SUPPLIER_NUMBER",
    "SUPPLIER_CUSTMER_NAME",
    "GL_LINE_DESCRIPTION",
    "PO_NUMBER",
    "NET_ACCOUNTED",
    "TRANSACTION_TYPE_NAME",
    "GL_TAX",
    "SUBLEDGER_TAX_CODE",
    "EMPLOYEE_NAME",
)


def _cell(value: str | None) -> str:
    if value is None:
        return ""
    return value


def _nonempty_stripped(value: str | None) -> bool:
    return bool(_cell(value).strip())


def decode_gl_export_bytes(raw: bytes, *, preferred: str | None = None) -> str:
    """Decode GL export bytes: strict ``preferred``, or UTF-8 then Windows-1252 / Latin-1."""
    if preferred:
        return raw.decode(preferred)
    for enc in ("utf-8-sig", "utf-8", "cp1252", "iso8859-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def gl_row_passes_filters(row: dict[str, str | None]) -> bool:
    """Invoice-related lines only; drop preliminary AnkReg booking lines."""
    if not (
        _nonempty_stripped(row.get("SUPPLIER_NUMBER"))
        or _nonempty_stripped(row.get("SUPPLIER_CUSTMER_NAME"))
    ):
        return False
    desc = _cell(row.get("GL_LINE_DESCRIPTION")).lower()
    if "ankreg" in desc:
        return False
    return True


def gl_lines_schema() -> list[bigquery.SchemaField]:
    fields = [
        bigquery.SchemaField(name, "STRING", mode="NULLABLE")
        for name in GL_EXPORT_COLUMNS
    ]
    fields.append(bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"))
    fields.append(
        bigquery.SchemaField(
            "loaded_at",
            "TIMESTAMP",
            mode="NULLABLE",
        )
    )
    return fields


def ensure_gl_lines_table(
    client: BigQueryClient,
    dataset_id: str,
    table_id: str,
    *,
    project_id: str | None = None,
    location: str = "US",
) -> str:
    """Create dataset and table if missing. Returns ``project.dataset.table``."""
    pid = project_id or client.project
    dataset_ref = bigquery.Dataset(f"{pid}.{dataset_id}")
    dataset_ref.location = location
    client.create_dataset(dataset_ref, exists_ok=True)
    table_ref = f"{pid}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=gl_lines_schema())
    client.create_table(table, exists_ok=True)
    return table_ref


def iter_filtered_gl_rows(
    path: Path,
    *,
    encoding: str | None = None,
) -> Iterator[dict[str, str]]:
    """Yield filtered rows as string dicts including ``source_file`` and ``loaded_at``.

    If ``encoding`` is set, the file is decoded with that codec only. Otherwise bytes are
    decoded with :func:`decode_gl_export_bytes` (UTF-8 first, then cp1252 / Latin-1).
    """
    path = path.resolve()
    dt = datetime.now(timezone.utc)
    loaded_at = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{dt.microsecond:06d} UTC"
    source_file = path.name
    blob = path.read_bytes()
    text = decode_gl_export_bytes(blob, preferred=encoding)
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    for raw in reader:
        row = {k: _cell(raw.get(k)) for k in GL_EXPORT_COLUMNS}
        if not gl_row_passes_filters(row):
            continue
        row["source_file"] = source_file
        row["loaded_at"] = loaded_at
        yield row


def _gl_load_job_config(
    write_disposition: str,
) -> bigquery.LoadJobConfig:
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="\t",
        skip_leading_rows=0,
        write_disposition=write_disposition,
        schema=gl_lines_schema(),
    )


def _run_load_job(job: bigquery.LoadJob) -> None:
    try:
        job.result()
    except Exception:
        for err in getattr(job, "errors", None) or ():
            logger.error("BigQuery load job error: %s", err)
        raise


def load_gl_txt_paths(
    client: BigQueryClient,
    table_ref: str,
    paths: list[Path],
    *,
    write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    location: str = "US",
    file_encoding: str | None = None,
) -> bigquery.LoadJob:
    """Load one or more GL ``*.txt`` files after filtering into ``table_ref``."""
    fieldnames = list(GL_EXPORT_COLUMNS) + ["source_file", "loaded_at"]
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=fieldnames,
        delimiter="\t",
        lineterminator="\n",
        quoting=csv.QUOTE_MINIMAL,
    )
    for p in paths:
        for row in iter_filtered_gl_rows(p, encoding=file_encoding):
            writer.writerow(row)
    data = buf.getvalue().encode("utf-8")
    job_config = _gl_load_job_config(write_disposition)
    job = client.load_table_from_file(
        io.BytesIO(data),
        table_ref,
        job_config=job_config,
        rewind=True,
        location=location,
    )
    _run_load_job(job)
    return job


def collect_gl_txt_files(gl_dir: Path) -> list[Path]:
    """Sorted ``*.txt`` files directly under ``gl_dir`` (non-recursive)."""
    if not gl_dir.is_dir():
        return []
    return sorted(p for p in gl_dir.glob("*.txt") if p.is_file())
