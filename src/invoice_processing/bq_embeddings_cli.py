"""CLI: BigQuery views/tables for invoice embeddings, backfill, and similarity search."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from google.cloud import bigquery

from invoice_processing.bq_invoice_embeddings import (
    DEFAULT_EMBEDDING_ENDPOINT,
    DEFAULT_OUTPUT_DIMENSIONALITY,
    build_backfill_embeddings_insert_sql,
    build_create_connection_ddl,
    build_create_remote_embedding_model_ddl,
    build_create_vector_index_ddl,
    build_invoice_embed_text_view_ddl,
    build_invoice_gl_context_view_ddl,
    build_rag_neighbors_with_gl_sql,
    build_vector_search_by_gcs_uri_sql,
    ensure_invoice_embeddings_table,
    run_ddl,
)

logger = logging.getLogger(__name__)


def _require(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        print(f"error: set {name}", file=sys.stderr)
        sys.exit(1)
    return v


def _optional(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v or default


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="BigQuery ML invoice embeddings: DDL, backfill, VECTOR_SEARCH.",
    )
    parser.add_argument(
        "command",
        choices=[
            "print-ddl",
            "ensure-embeddings-table",
            "create-embed-text-view",
            "create-gl-context-view",
            "create-connection-sql",
            "create-remote-model-sql",
            "create-vector-index-sql",
            "backfill",
            "search",
            "rag-search",
        ],
        help="Action to run.",
    )
    parser.add_argument(
        "--gcs-uri",
        help="For search / rag-search: source invoice gcs_uri.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Neighbor count for search / rag-search (default 10).",
    )
    args = parser.parse_args()

    project_id = _require("GCP_PROJECT")
    bq_dataset = _require("BQ_DATASET")
    bq_location = _optional("BQ_LOCATION", "US")
    extractions_table = _optional("BQ_INVOICE_EXTRACTIONS_TABLE", "invoice_extractions")
    embeddings_table = _optional("BQ_INVOICE_EMBEDDINGS_TABLE", "invoice_embeddings")
    embed_view = _optional("BQ_INVOICE_EMBED_TEXT_VIEW", "v_invoice_embed_text")
    gl_table = _optional("BQ_GL_LINES_TABLE", "gl_lines")
    gl_context_view = _optional("BQ_INVOICE_GL_CONTEXT_VIEW", "v_invoice_gl_context")
    remote_model = _optional("BQ_EMBEDDING_REMOTE_MODEL", "bqml_invoice_embedding")
    connection_region = _optional("BQ_VERTEX_CONNECTION_REGION", "us-central1")
    connection_id = _optional("BQ_VERTEX_CONNECTION_ID", "vertex-ai")
    endpoint = _optional("BQ_EMBEDDING_ENDPOINT", DEFAULT_EMBEDDING_ENDPOINT)
    out_dim = int(_optional("BQ_EMBEDDING_OUTPUT_DIMENSIONALITY", str(DEFAULT_OUTPUT_DIMENSIONALITY)))

    if args.command == "print-ddl":
        print(build_create_connection_ddl(
            project_id=project_id,
            connection_region=connection_region,
            connection_id=connection_id,
        ))
        print(";\n")
        print(build_create_remote_embedding_model_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            model_id=remote_model,
            connection_region=connection_region,
            connection_id=connection_id,
            endpoint=endpoint,
        ))
        print(";\n")
        print(build_invoice_embed_text_view_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            extractions_table_id=extractions_table,
            view_id=embed_view,
        ))
        print(";\n")
        print(build_invoice_gl_context_view_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            gl_table_id=gl_table,
            view_id=gl_context_view,
        ))
        print(";\n")
        print(build_create_vector_index_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            table_id=embeddings_table,
        ))
        return

    if args.command == "create-connection-sql":
        print(build_create_connection_ddl(
            project_id=project_id,
            connection_region=connection_region,
            connection_id=connection_id,
        ))
        return

    if args.command == "create-remote-model-sql":
        print(build_create_remote_embedding_model_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            model_id=remote_model,
            connection_region=connection_region,
            connection_id=connection_id,
            endpoint=endpoint,
        ))
        return

    if args.command == "create-vector-index-sql":
        print(build_create_vector_index_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            table_id=embeddings_table,
        ))
        return

    client = bigquery.Client(project=project_id, location=bq_location)

    if args.command == "ensure-embeddings-table":
        ref = ensure_invoice_embeddings_table(
            client,
            bq_dataset,
            embeddings_table,
            project_id=project_id,
            location=bq_location,
            embedding_dimensions=out_dim,
        )
        logger.info("Embeddings table ready: %s", ref)
        return

    if args.command == "create-embed-text-view":
        sql = build_invoice_embed_text_view_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            extractions_table_id=extractions_table,
            view_id=embed_view,
        )
        run_ddl(client, sql, location=bq_location)
        logger.info("View %s.%s.%s created", project_id, bq_dataset, embed_view)
        return

    if args.command == "create-gl-context-view":
        sql = build_invoice_gl_context_view_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            gl_table_id=gl_table,
            view_id=gl_context_view,
        )
        run_ddl(client, sql, location=bq_location)
        logger.info("View %s.%s.%s created", project_id, bq_dataset, gl_context_view)
        return

    if args.command == "backfill":
        sql = build_backfill_embeddings_insert_sql(
            project_id=project_id,
            dataset_id=bq_dataset,
            embed_text_view_id=embed_view,
            embeddings_table_id=embeddings_table,
            remote_model_id=remote_model,
            endpoint_literal=endpoint,
            output_dimensionality=out_dim,
        )
        job = client.query(sql, location=bq_location)
        job.result()
        logger.info("Backfill job %s done; bytes %s", job.job_id, job.total_bytes_processed)
        return

    if args.command == "search":
        if not args.gcs_uri:
            print("error: --gcs-uri required", file=sys.stderr)
            sys.exit(1)
        sql = build_vector_search_by_gcs_uri_sql(
            project_id=project_id,
            dataset_id=bq_dataset,
            query_gcs_uri=args.gcs_uri,
            embed_text_view_id=embed_view,
            embeddings_table_id=embeddings_table,
            remote_model_id=remote_model,
            top_k=args.top_k,
            output_dimensionality=out_dim,
        )
        for row in client.query(sql, location=bq_location).result():
            print(dict(row))
        return

    if args.command == "rag-search":
        if not args.gcs_uri:
            print("error: --gcs-uri required", file=sys.stderr)
            sys.exit(1)
        sql = build_rag_neighbors_with_gl_sql(
            project_id=project_id,
            dataset_id=bq_dataset,
            query_gcs_uri=args.gcs_uri,
            embed_text_view_id=embed_view,
            embeddings_table_id=embeddings_table,
            gl_context_view_id=gl_context_view,
            remote_model_id=remote_model,
            top_k=args.top_k,
            output_dimensionality=out_dim,
        )
        for row in client.query(sql, location=bq_location).result():
            print(dict(row))
        return


if __name__ == "__main__":
    main()
