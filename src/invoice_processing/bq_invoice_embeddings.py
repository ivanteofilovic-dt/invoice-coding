"""BigQuery ML invoice embeddings: embed text view, storage table, backfill, VECTOR_SEARCH.

Two ways to fill ``invoice_embeddings``:

1. **In-BigQuery** — ``AI.GENERATE_EMBEDDING`` on a remote model (requires a BigQuery
   **connection** and Vertex access for that connection’s service account).

2. **Outside BigQuery** — compute vectors with the Vertex **REST/SDK**, a Colab notebook,
   Cloud Run, etc., using any principal you control, then **load** rows into
   ``invoice_embeddings`` via :func:`load_precomputed_embedding_ndjson_files`.
   **``VECTOR_SEARCH`` does not use the connection**; it only reads stored arrays.
   Use :func:`build_vector_search_by_stored_embedding_sql` / ``search-stored`` so the
   query vector is read from the same table (no embedding API call in SQL).
"""

from __future__ import annotations

import io
import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from google.cloud.bigquery import Client as BigQueryClient

# Version bump when the CONCAT / line serialization recipe changes (triggers re-embed).
EMBED_TEXT_VERSION = "v1"

# Default Vertex text embedding endpoint for CREATE REMOTE MODEL (override via env in CLI).
DEFAULT_EMBEDDING_ENDPOINT = "text-embedding-004"

# Must match output_dimensionality passed to AI.GENERATE_EMBEDDING for text-embedding models (max 768).
DEFAULT_OUTPUT_DIMENSIONALITY = 768

# BigQuery quota: IVF (and TreeAH) vector indexes cannot be created on tiny tables.
# Below this row count, use ``VECTOR_SEARCH`` without an index (exact / brute-force).
# See https://docs.cloud.google.com/bigquery/quotas#vector-index-limits
IVF_VECTOR_INDEX_MIN_ROW_COUNT = 5000

_DEFAULT_EXTRACTIONS_TABLE = "invoice_extractions"
_DEFAULT_EMBEDDINGS_TABLE = "invoice_embeddings"
_DEFAULT_EMBED_TEXT_VIEW = "v_invoice_embed_text"
_DEFAULT_GL_LINES_TABLE = "gl_lines"
_DEFAULT_GL_CONTEXT_VIEW = "v_invoice_gl_context"
_DEFAULT_REMOTE_MODEL = "bqml_invoice_embedding"

# Hyphens are valid in GCP project IDs (and some connection ids); dataset/table
# ids are typically alphanumeric + underscore only—hyphenated datasets fail at BQ.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,1023}$")
_CONNECTION_REGION_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _qualified(project_id: str, dataset_id: str, table_or_view_id: str) -> str:
    return f"`{_assert_ident(project_id)}.{_assert_ident(dataset_id)}.{_assert_ident(table_or_view_id)}`"


def _connection_qualified(project_id: str, region: str, connection_id: str) -> str:
    """Fully qualified BigQuery connection id (region may contain hyphens, e.g. us-central1)."""
    pid = _assert_ident(project_id)
    cid = _assert_ident(connection_id)
    if not _CONNECTION_REGION_RE.match(region):
        msg = f"Invalid connection region: {region!r}"
        raise ValueError(msg)
    return f"`{pid}.{region}.{cid}`"


def _assert_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        msg = f"Invalid BigQuery identifier: {name!r}"
        raise ValueError(msg)
    return name


def invoice_embed_text_version() -> str:
    return EMBED_TEXT_VERSION


def invoice_embedding_inner_select_sql(
    extractions_ref: str,
) -> str:
    """SQL fragment: single-row select list body from *invoice_extractions* (no outer SELECT).

    ``extractions_ref`` must be a fully qualified escaped table id, e.g. `` `proj.ds.invoice_extractions` ``.
    """
    return f"""
  SELECT
    e.gcs_uri,
    CONCAT(
      'supplier_legal:', COALESCE(e.supplier.legal_name, ''), '\\n',
      'supplier_name:', COALESCE(e.supplier.name, ''), '\\n',
      'supplier_vat:', COALESCE(e.supplier.vat_number, ''), '\\n',
      'supplier_company_id:', COALESCE(e.supplier.company_legal_id, ''), '\\n',
      'supplier_party_id:', COALESCE(e.supplier.party_identification, ''), '\\n',
      'customer_name:', COALESCE(e.customer.name, ''), '\\n',
      'invoice_number:', COALESCE(e.invoice_number, ''), '\\n',
      'buyer_order:', COALESCE(e.buyer_order_number, ''), '\\n',
      'currency:', COALESCE(e.currency_code, ''), '\\n',
      'issue_date:', COALESCE(CAST(e.issue_date AS STRING), ''), '\\n',
      'due_date:', COALESCE(CAST(e.due_date AS STRING), ''), '\\n',
      'header_note:', COALESCE(e.header_note, ''), '\\n',
      'delivery:', COALESCE(e.delivery.delivery_location, ''), '\\n',
      'totals_excl:', COALESCE(CAST(e.document_totals.total_amount_excl_tax AS STRING), ''),
      '|tax:', COALESCE(CAST(e.document_totals.total_tax_amount AS STRING), ''),
      '|incl:', COALESCE(CAST(e.document_totals.total_amount_incl_tax AS STRING), ''), '\\n',
      'lines:',
      COALESCE(
        ARRAY_TO_STRING(
          ARRAY(
            SELECT
              FORMAT(
                '%d|%s|%s|%s|%s',
                COALESCE(il.line_number, 0),
                COALESCE(il.item_name, ''),
                COALESCE(il.item_number, ''),
                COALESCE(CAST(il.line_amount AS STRING), ''),
                COALESCE(il.line_note, '')
              )
            FROM UNNEST(e.invoice_lines) AS il
            ORDER BY il.line_number NULLS LAST, il.item_name NULLS LAST
          ),
          '; '
        ),
        ''
      ),
      '\\n',
      'tax_lines:',
      COALESCE(
        ARRAY_TO_STRING(
          ARRAY(
            SELECT
              FORMAT(
                '%s|%s|%s|%s',
                COALESCE(tl.tax_category, ''),
                COALESCE(CAST(tl.tax_rate_percent AS STRING), ''),
                COALESCE(CAST(tl.taxable_amount AS STRING), ''),
                COALESCE(CAST(tl.tax_amount AS STRING), '')
              )
            FROM UNNEST(e.tax_lines) AS tl
          ),
          '; '
        ),
        ''
      ),
      '\\n',
      'extras:',
      COALESCE(SUBSTR(TO_JSON_STRING(e.extras), 1, 4000), '')
    ) AS embed_text,
    UPPER(REGEXP_REPLACE(TRIM(COALESCE(e.invoice_number, '')), r'\\s+', '')) AS invoice_number_norm,
    UPPER(REGEXP_REPLACE(TRIM(COALESCE(e.invoice_number, '')), r'\\s+', '')) AS invoice_key_norm,
    TRIM(COALESCE(
      e.supplier.company_legal_id,
      e.supplier.party_identification,
      e.supplier.endpoint_id,
      ''
    )) AS supplier_ref_norm,
    e.extracted_at,
    e.invoice_number,
    e.currency_code
  FROM {extractions_ref} AS e
"""


def build_invoice_embed_text_view_ddl(
    *,
    project_id: str,
    dataset_id: str,
    extractions_table_id: str = _DEFAULT_EXTRACTIONS_TABLE,
    view_id: str = _DEFAULT_EMBED_TEXT_VIEW,
) -> str:
    """CREATE OR REPLACE VIEW for deterministic embed_text + join keys (matches :data:`EMBED_TEXT_VERSION`)."""
    ext = _qualified(project_id, dataset_id, extractions_table_id)
    view = _qualified(project_id, dataset_id, view_id)
    inner = invoice_embedding_inner_select_sql(ext).strip()
    ver = EMBED_TEXT_VERSION.replace("'", "''")
    return f"""
CREATE OR REPLACE VIEW {view} AS
WITH base AS (
{inner}
)
SELECT
  gcs_uri,
  embed_text,
  FARM_FINGERPRINT(embed_text) AS content_hash,
  '{ver}' AS embed_text_version,
  NULLIF(invoice_key_norm, '') AS invoice_key_norm,
  NULLIF(invoice_number_norm, '') AS invoice_number_norm,
  NULLIF(supplier_ref_norm, '') AS supplier_ref_norm,
  extracted_at,
  invoice_number,
  currency_code
FROM base
WHERE LENGTH(TRIM(embed_text)) > 0
""".strip()


def build_invoice_gl_context_view_ddl(
    *,
    project_id: str,
    dataset_id: str,
    gl_table_id: str = _DEFAULT_GL_LINES_TABLE,
    view_id: str = _DEFAULT_GL_CONTEXT_VIEW,
) -> str:
    """Aggregate GL lines per invoice + supplier for RAG metadata (non-vector)."""
    gl = _qualified(project_id, dataset_id, gl_table_id)
    view = _qualified(project_id, dataset_id, view_id)
    return f"""
CREATE OR REPLACE VIEW {view} AS
SELECT
  UPPER(REGEXP_REPLACE(TRIM(COALESCE(INVOICE_NUM, '')), r'\\s+', '')) AS invoice_key_norm,
  TRIM(COALESCE(SUPPLIER_NUMBER, '')) AS supplier_number_norm,
  COUNT(*) AS gl_line_count,
  SUM(SAFE_CAST(NULLIF(TRIM(NET_ACCOUNTED), '') AS NUMERIC)) AS net_accounted_sum,
  ARRAY_AGG(
    STRUCT(
      BOOKING_DATE AS booking_date,
      PERIOD AS period,
      ACCOUNT AS account,
      HFM_ACCOUNT AS hfm_account,
      GL_LINE_DESCRIPTION AS gl_line_description,
      DEPARTMENT AS department,
      PRODUCT AS product,
      NET_ACCOUNTED AS net_accounted,
      TRANSACTION_TYPE_NAME AS transaction_type_name
    )
    ORDER BY BOOKING_DATE DESC, JOURNAL_NUMBER DESC
    LIMIT 80
  ) AS gl_lines_recent
FROM {gl}
WHERE NULLIF(TRIM(COALESCE(INVOICE_NUM, '')), '') IS NOT NULL
  AND NULLIF(TRIM(COALESCE(SUPPLIER_NUMBER, '')), '') IS NOT NULL
GROUP BY 1, 2
""".strip()


def invoice_embeddings_schema(
    *,
    embedding_dimensions: int = DEFAULT_OUTPUT_DIMENSIONALITY,
) -> list[bigquery.SchemaField]:
    """Schema for ``invoice_embeddings`` (fixed-length embedding array)."""
    _ = embedding_dimensions  # documented contract; BQ does not enforce array length in schema
    return [
        bigquery.SchemaField("gcs_uri", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("embed_text", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("content_hash", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("embed_text_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "embedding",
            "FLOAT64",
            mode="REPEATED",
        ),
        bigquery.SchemaField("embedding_model", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("embedding_endpoint", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("output_dimensionality", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("embedded_at", "TIMESTAMP", mode="NULLABLE"),
    ]


def ensure_invoice_embeddings_table(
    client: BigQueryClient,
    dataset_id: str,
    table_id: str = _DEFAULT_EMBEDDINGS_TABLE,
    *,
    project_id: str | None = None,
    location: str = "US",
    embedding_dimensions: int = DEFAULT_OUTPUT_DIMENSIONALITY,
) -> str:
    """Create dataset if needed and embeddings table. Returns ``project.dataset.table``."""
    pid = project_id or client.project
    dataset_ref = bigquery.Dataset(f"{pid}.{dataset_id}")
    dataset_ref.location = location
    client.create_dataset(dataset_ref, exists_ok=True)
    table_ref = f"{pid}.{dataset_id}.{table_id}"
    table = bigquery.Table(
        table_ref,
        schema=invoice_embeddings_schema(embedding_dimensions=embedding_dimensions),
    )
    client.create_table(table, exists_ok=True)
    return table_ref


def build_create_connection_ddl(
    *,
    project_id: str,
    connection_region: str,
    connection_id: str,
) -> str:
    """DDL for a CLOUD_RESOURCE connection (run in BigQuery SQL). Region must match embedding workload."""
    conn = _connection_qualified(project_id, connection_region, connection_id)
    return f"""
CREATE CONNECTION IF NOT EXISTS {conn}
OPTIONS (
  connection_type = 'CLOUD_RESOURCE',
  friendly_name = 'Vertex AI for BigQuery ML embeddings',
  description = 'Used by remote embedding models and AI.GENERATE_EMBEDDING'
)
""".strip()


def build_create_remote_embedding_model_ddl(
    *,
    project_id: str,
    dataset_id: str,
    model_id: str = _DEFAULT_REMOTE_MODEL,
    connection_region: str,
    connection_id: str,
    endpoint: str = DEFAULT_EMBEDDING_ENDPOINT,
) -> str:
    """CREATE OR REPLACE remote model over a Vertex text embedding endpoint."""
    model = _qualified(project_id, dataset_id, model_id)
    conn = _connection_qualified(project_id, connection_region, connection_id)
    ep = endpoint.replace("'", "''")
    return f"""
CREATE OR REPLACE MODEL {model}
REMOTE WITH CONNECTION {conn}
OPTIONS (ENDPOINT = '{ep}')
""".strip()


def build_embeddings_table_health_sql(
    *,
    project_id: str,
    dataset_id: str,
    table_id: str = _DEFAULT_EMBEDDINGS_TABLE,
) -> str:
    """Aggregate stats for diagnosing vector index / ``VECTOR_SEARCH`` issues."""
    tbl = _qualified(project_id, dataset_id, table_id)
    return f"""
SELECT
  COUNT(*) AS row_count,
  COUNTIF(embedding IS NULL) AS null_embedding_rows,
  COUNTIF(embedding IS NOT NULL AND ARRAY_LENGTH(embedding) > 0) AS rows_with_vectors,
  MIN(ARRAY_LENGTH(embedding)) AS min_array_length,
  MAX(ARRAY_LENGTH(embedding)) AS max_array_length
FROM {tbl}
""".strip()


def build_create_vector_index_ddl(
    *,
    project_id: str,
    dataset_id: str,
    table_id: str = _DEFAULT_EMBEDDINGS_TABLE,
    index_id: str = "invoice_embeddings_ivf",
) -> str:
    """Optional IVF vector index on the embedding column (scale / ANN).

    BigQuery requires the base table to have at least :data:`IVF_VECTOR_INDEX_MIN_ROW_COUNT`
    rows to create an IVF index; smaller tables should use :func:`build_vector_search_by_gcs_uri_sql`
    / :func:`build_vector_search_by_stored_embedding_sql` only (``VECTOR_SEARCH`` uses brute
    force when no index exists).

    Also requires non-NULL ``embedding`` arrays; see :func:`build_embeddings_table_health_sql`.
    """
    tbl = _qualified(project_id, dataset_id, table_id)
    idx = _qualified(project_id, dataset_id, index_id)
    return f"""
CREATE VECTOR INDEX IF NOT EXISTS {idx}
ON {tbl}(embedding)
OPTIONS (
  index_type = 'IVF',
  distance_type = 'COSINE',
  ivf_options = '{{"num_lists": 100}}'
)
""".strip()


def build_backfill_embeddings_insert_sql(
    *,
    project_id: str,
    dataset_id: str,
    embed_text_view_id: str = _DEFAULT_EMBED_TEXT_VIEW,
    embeddings_table_id: str = _DEFAULT_EMBEDDINGS_TABLE,
    remote_model_id: str = _DEFAULT_REMOTE_MODEL,
    endpoint_literal: str = DEFAULT_EMBEDDING_ENDPOINT,
    output_dimensionality: int = DEFAULT_OUTPUT_DIMENSIONALITY,
) -> str:
    """INSERT new embeddings for rows in the embed view missing from ``invoice_embeddings`` (by content_hash)."""
    view = _qualified(project_id, dataset_id, embed_text_view_id)
    emb_tbl = _qualified(project_id, dataset_id, embeddings_table_id)
    model = _qualified(project_id, dataset_id, remote_model_id)
    ep = endpoint_literal.replace("'", "''")
    return f"""
INSERT INTO {emb_tbl} (
  gcs_uri,
  embed_text,
  content_hash,
  embed_text_version,
  embedding,
  embedding_model,
  embedding_endpoint,
  output_dimensionality,
  embedded_at
)
WITH pending AS (
  SELECT
    v.gcs_uri,
    v.embed_text,
    v.content_hash,
    v.embed_text_version,
    COALESCE(v.invoice_number, v.gcs_uri) AS title
  FROM {view} AS v
  WHERE NOT EXISTS (
    SELECT 1
    FROM {emb_tbl} AS e
    WHERE e.gcs_uri = v.gcs_uri
      AND e.content_hash = v.content_hash
      AND e.embed_text_version = v.embed_text_version
  )
),
generated AS (
  SELECT
    gcs_uri,
    embed_text_version,
    content_hash,
    embedding
  FROM AI.GENERATE_EMBEDDING(
    MODEL {model},
    (
      SELECT
        gcs_uri,
        embed_text_version,
        content_hash,
        embed_text AS content,
        title
      FROM pending
    ),
    STRUCT(
      'RETRIEVAL_DOCUMENT' AS task_type,
      {int(output_dimensionality)} AS output_dimensionality
    )
  )
)
SELECT
  p.gcs_uri,
  p.embed_text,
  p.content_hash,
  p.embed_text_version,
  g.embedding,
  '{_assert_ident(remote_model_id)}' AS embedding_model,
  '{ep}' AS embedding_endpoint,
  {int(output_dimensionality)} AS output_dimensionality,
  CURRENT_TIMESTAMP() AS embedded_at
FROM generated AS g
INNER JOIN pending AS p
  ON p.gcs_uri = g.gcs_uri
WHERE g.embedding IS NOT NULL
  AND ARRAY_LENGTH(g.embedding) > 0
""".strip()


def build_vector_search_by_stored_embedding_sql(
    *,
    project_id: str,
    dataset_id: str,
    query_gcs_uri: str,
    embeddings_table_id: str = _DEFAULT_EMBEDDINGS_TABLE,
    top_k: int = 10,
) -> str:
    """Top-K similar rows using the query invoice's vector already stored in ``invoice_embeddings``.

    No remote model or BigQuery connection is used. Fails at query time if ``gcs_uri`` has no row.
    """
    emb_tbl = _qualified(project_id, dataset_id, embeddings_table_id)
    uri_esc = query_gcs_uri.replace("'", "''")
    return f"""
WITH qemb AS (
  SELECT embedding
  FROM {emb_tbl}
  WHERE gcs_uri = '{uri_esc}'
  LIMIT 1
)
SELECT
  gcs_uri,
  embed_text_version,
  content_hash,
  distance
FROM (
  SELECT
    gcs_uri,
    embed_text_version,
    content_hash,
    distance
  FROM VECTOR_SEARCH(
    TABLE {emb_tbl},
    'embedding',
    (SELECT embedding FROM qemb),
    distance_type => 'COSINE',
    top_k => {int(top_k) + 10}
  )
)
WHERE gcs_uri != '{uri_esc}'
ORDER BY distance
LIMIT {int(top_k)}
""".strip()


def build_rag_neighbors_with_gl_stored_embedding_sql(
    *,
    project_id: str,
    dataset_id: str,
    query_gcs_uri: str,
    embed_text_view_id: str = _DEFAULT_EMBED_TEXT_VIEW,
    embeddings_table_id: str = _DEFAULT_EMBEDDINGS_TABLE,
    gl_context_view_id: str = _DEFAULT_GL_CONTEXT_VIEW,
    top_k: int = 10,
) -> str:
    """Like :func:`build_rag_neighbors_with_gl_sql` but query vector comes from stored embeddings only."""
    base = build_vector_search_by_stored_embedding_sql(
        project_id=project_id,
        dataset_id=dataset_id,
        query_gcs_uri=query_gcs_uri,
        embeddings_table_id=embeddings_table_id,
        top_k=top_k,
    )
    v = _qualified(project_id, dataset_id, embed_text_view_id)
    glv = _qualified(project_id, dataset_id, gl_context_view_id)
    inner = base.strip().rstrip(";")
    return f"""
WITH hits AS (
  ({inner})
),
ev AS (
  SELECT gcs_uri, invoice_key_norm, supplier_ref_norm
  FROM {v}
)
SELECT
  h.*,
  g.gl_line_count,
  g.net_accounted_sum,
  g.gl_lines_recent
FROM hits AS h
LEFT JOIN ev ON ev.gcs_uri = h.gcs_uri
LEFT JOIN {glv} AS g
  ON g.invoice_key_norm = ev.invoice_key_norm
 AND g.supplier_number_norm = ev.supplier_ref_norm
ORDER BY h.distance
""".strip()


def _embedding_load_job_config(write_disposition: str) -> bigquery.LoadJobConfig:
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        schema=invoice_embeddings_schema(),
    )


def _run_embedding_load_job(job: bigquery.LoadJob) -> None:
    try:
        job.result()
    except Exception:
        for err in getattr(job, "errors", None) or ():
            logger.error("BigQuery load job error: %s", err)
        raise


def _drop_null_json_values_embedding(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if v is None or v == "null":
                continue
            out[k] = _drop_null_json_values_embedding(v)
        return out
    if isinstance(value, list):
        return [_drop_null_json_values_embedding(v) for v in value if v is not None]
    return value


def _sanitize_precomputed_embedding_row(row: dict[str, Any]) -> dict[str, Any]:
    """NDJSON row for ``invoice_embeddings``; ``gcs_uri`` and ``embedding`` (number array) required."""
    emb = row.get("embedding")
    if not isinstance(emb, list) or not emb:
        msg = "row must include non-empty 'embedding' array"
        raise ValueError(msg)
    if not row.get("gcs_uri"):
        msg = "row must include 'gcs_uri'"
        raise ValueError(msg)
    cleaned = _drop_null_json_values_embedding(dict(row))
    cleaned["embedding"] = [float(x) for x in emb]
    return cleaned


def load_precomputed_embedding_ndjson_rows(
    client: BigQueryClient,
    table_ref: str,
    rows: list[dict[str, Any]],
    *,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    location: str = "US",
) -> bigquery.LoadJob:
    """Load precomputed embedding rows (no BigQuery ML / connection)."""
    buf = io.BytesIO()
    for row in rows:
        sanitized = _sanitize_precomputed_embedding_row(row)
        buf.write(json.dumps(sanitized, default=str).encode("utf-8"))
        buf.write(b"\n")
    buf.seek(0)
    job_config = _embedding_load_job_config(write_disposition)
    job = client.load_table_from_file(
        buf, table_ref, job_config=job_config, rewind=True, location=location
    )
    _run_embedding_load_job(job)
    return job


def load_precomputed_embedding_ndjson_files(
    client: BigQueryClient,
    table_ref: str,
    paths: list[str | Path],
    *,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    location: str = "US",
) -> list[bigquery.LoadJob]:
    """Load one or more local NDJSON files into ``invoice_embeddings``."""
    job_config = _embedding_load_job_config(write_disposition)
    jobs: list[bigquery.LoadJob] = []
    for path in paths:
        p = Path(path)
        with p.open("rb") as f:
            job = client.load_table_from_file(
                f, table_ref, job_config=job_config, rewind=True, location=location
            )
        _run_embedding_load_job(job)
        jobs.append(job)
    return jobs


def build_vector_search_by_gcs_uri_sql(
    *,
    project_id: str,
    dataset_id: str,
    query_gcs_uri: str,
    embed_text_view_id: str = _DEFAULT_EMBED_TEXT_VIEW,
    embeddings_table_id: str = _DEFAULT_EMBEDDINGS_TABLE,
    remote_model_id: str = _DEFAULT_REMOTE_MODEL,
    top_k: int = 10,
    output_dimensionality: int = DEFAULT_OUTPUT_DIMENSIONALITY,
) -> str:
    """Top-K similar invoices by cosine distance; excludes the same gcs_uri."""
    view = _qualified(project_id, dataset_id, embed_text_view_id)
    emb_tbl = _qualified(project_id, dataset_id, embeddings_table_id)
    model = _qualified(project_id, dataset_id, remote_model_id)
    uri_esc = query_gcs_uri.replace("'", "''")
    return f"""
WITH qtext AS (
  SELECT embed_text AS content, COALESCE(invoice_number, gcs_uri) AS title
  FROM {view}
  WHERE gcs_uri = '{uri_esc}'
  LIMIT 1
),
qemb AS (
  SELECT embedding
  FROM AI.GENERATE_EMBEDDING(
    MODEL {model},
    (SELECT * FROM qtext),
    STRUCT(
      'RETRIEVAL_QUERY' AS task_type,
      {int(output_dimensionality)} AS output_dimensionality
    )
  )
)
SELECT
  gcs_uri,
  embed_text_version,
  content_hash,
  distance
FROM (
  SELECT
    gcs_uri,
    embed_text_version,
    content_hash,
    distance
  FROM VECTOR_SEARCH(
    TABLE {emb_tbl},
    'embedding',
    (SELECT embedding FROM qemb),
    distance_type => 'COSINE',
    top_k => {int(top_k) + 10}
  )
)
WHERE gcs_uri != '{uri_esc}'
ORDER BY distance
LIMIT {int(top_k)}
""".strip()


def build_rag_neighbors_with_gl_sql(
    *,
    project_id: str,
    dataset_id: str,
    query_gcs_uri: str,
    embed_text_view_id: str = _DEFAULT_EMBED_TEXT_VIEW,
    embeddings_table_id: str = _DEFAULT_EMBEDDINGS_TABLE,
    gl_context_view_id: str = _DEFAULT_GL_CONTEXT_VIEW,
    remote_model_id: str = _DEFAULT_REMOTE_MODEL,
    top_k: int = 10,
    output_dimensionality: int = DEFAULT_OUTPUT_DIMENSIONALITY,
) -> str:
    """VECTOR_SEARCH neighbors joined to GL context on invoice + supplier keys."""
    base = build_vector_search_by_gcs_uri_sql(
        project_id=project_id,
        dataset_id=dataset_id,
        query_gcs_uri=query_gcs_uri,
        embed_text_view_id=embed_text_view_id,
        embeddings_table_id=embeddings_table_id,
        remote_model_id=remote_model_id,
        top_k=top_k,
        output_dimensionality=output_dimensionality,
    )
    v = _qualified(project_id, dataset_id, embed_text_view_id)
    glv = _qualified(project_id, dataset_id, gl_context_view_id)
    inner = base.strip().rstrip(";")
    return f"""
WITH hits AS (
  ({inner})
),
ev AS (
  SELECT gcs_uri, invoice_key_norm, supplier_ref_norm
  FROM {v}
)
SELECT
  h.*,
  g.gl_line_count,
  g.net_accounted_sum,
  g.gl_lines_recent
FROM hits AS h
LEFT JOIN ev ON ev.gcs_uri = h.gcs_uri
LEFT JOIN {glv} AS g
  ON g.invoice_key_norm = ev.invoice_key_norm
 AND g.supplier_number_norm = ev.supplier_ref_norm
ORDER BY h.distance
""".strip()


def run_ddl(
    client: BigQueryClient,
    sql: str,
    *,
    location: str | None = None,
) -> bigquery.QueryJob:
    job = client.query(sql, location=location)
    job.result()
    return job
