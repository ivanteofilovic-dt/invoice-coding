"""BigQuery table DDL and load for invoice extraction rows."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

from google.cloud import bigquery

if TYPE_CHECKING:
    from google.cloud.bigquery import Client as BigQueryClient


def _contact_fields() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    ]


def _party_fields() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("address_line", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("postal_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("endpoint_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("party_identification", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_legal_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("legal_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vat_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tax_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "contact",
            "RECORD",
            mode="NULLABLE",
            fields=_contact_fields(),
        ),
    ]


def invoice_extractions_schema() -> list[bigquery.SchemaField]:
    """Schema for one row per PDF (matches plan / extraction JSON)."""
    tax_line = [
        bigquery.SchemaField("tax_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tax_rate_percent", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("taxable_amount", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("tax_amount", "NUMERIC", mode="NULLABLE"),
    ]
    inv_line = [
        bigquery.SchemaField("line_number", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("item_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("item_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("quantity", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("unit_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("net_unit_price", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("tax_details", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("allowance_charge", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("line_amount", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("line_note", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("orderline_reference", "STRING", mode="NULLABLE"),
    ]
    return [
        bigquery.SchemaField("gcs_uri", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("model_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("batch_job_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("issue_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("due_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("invoice_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("buyer_order_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("currency_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("header_note", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "supplier",
            "RECORD",
            mode="NULLABLE",
            fields=_party_fields(),
        ),
        bigquery.SchemaField(
            "customer",
            "RECORD",
            mode="NULLABLE",
            fields=_party_fields(),
        ),
        bigquery.SchemaField(
            "delivery",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("delivery_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("delivery_location", "STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "tax_lines",
            "RECORD",
            mode="REPEATED",
            fields=tax_line,
        ),
        bigquery.SchemaField(
            "payment",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("payment_terms", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("payment_means_code", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("instruction_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("account_number", "STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    "financial_institution_branch", "STRING", mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "financial_institution_name", "STRING", mode="NULLABLE"
                ),
                bigquery.SchemaField("payment_id", "STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "invoice_lines",
            "RECORD",
            mode="REPEATED",
            fields=inv_line,
        ),
        bigquery.SchemaField(
            "document_totals",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("rounding_amount", "NUMERIC", mode="NULLABLE"),
                bigquery.SchemaField(
                    "total_amount_excl_tax", "NUMERIC", mode="NULLABLE"
                ),
                bigquery.SchemaField("total_tax_amount", "NUMERIC", mode="NULLABLE"),
                bigquery.SchemaField(
                    "total_amount_incl_tax", "NUMERIC", mode="NULLABLE"
                ),
            ],
        ),
        bigquery.SchemaField("extras", "JSON", mode="NULLABLE"),
    ]


def ensure_invoice_extractions_table(
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
    table = bigquery.Table(table_ref, schema=invoice_extractions_schema())
    client.create_table(table, exists_ok=True)
    return table_ref


def load_ndjson_rows(
    client: BigQueryClient,
    table_ref: str,
    rows: list[dict[str, Any]],
    *,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    location: str = "US",
) -> bigquery.LoadJob:
    """Load newline-delimited JSON rows via an in-memory load job."""
    buf = io.BytesIO()
    for row in rows:
        buf.write(json.dumps(row, default=str).encode("utf-8"))
        buf.write(b"\n")
    buf.seek(0)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        schema=invoice_extractions_schema(),
    )
    job = client.load_table_from_file(
        buf, table_ref, job_config=job_config, rewind=True, location=location
    )
    job.result()
    return job
