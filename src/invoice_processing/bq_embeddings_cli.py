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
    build_rag_neighbors_with_gl_stored_embedding_sql,
    build_vector_search_by_gcs_uri_sql,
    build_vector_search_by_stored_embedding_sql,
    ensure_invoice_embeddings_table,
    load_precomputed_embedding_ndjson_files,
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
            "setup",
            "print-ddl",
            "ensure-embeddings-table",
            "create-embed-text-view",
            "create-gl-context-view",
            "create-connection-sql",
            "create-remote-model-sql",
            "create-vector-index-sql",
            "backfill",
            "import-embeddings",
            "search",
            "search-stored",
            "rag-search",
            "rag-search-stored",
        ],
        help="Action to run. Use 'setup' to create connection, remote embedding model, "
        "embeddings table, and views (everything needed before backfill).",
    )
    parser.add_argument(
        "--gcs-uri",
        help="For search / rag-search / search-stored: source invoice gcs_uri.",
    )
    parser.add_argument(
        "jsonl_files",
        nargs="*",
        help="For import-embeddings: NDJSON files with gcs_uri + embedding arrays.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="For import-embeddings: replace table contents (WRITE_TRUNCATE).",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Neighbor count for search / rag-search (default 10).",
    )
    parser.add_argument(
        "--skip-vertex-resources",
        action="store_true",
        help="For setup: skip CREATE CONNECTION and CREATE REMOTE MODEL (only if "
        "BQ_EMBEDDING_REMOTE_MODEL already exists in BQ_DATASET).",
    )
    parser.add_argument(
        "--with-vector-index",
        action="store_true",
        help="For setup: also CREATE VECTOR INDEX on invoice_embeddings (optional scale-up).",
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

    if args.command == "setup":
        if not args.skip_vertex_resources:
            conn_sql = build_create_connection_ddl(
                project_id=project_id,
                connection_region=connection_region,
                connection_id=connection_id,
            )
            try:
                run_ddl(client, conn_sql, location=connection_region)
            except Exception:
                logger.exception("CREATE CONNECTION failed")
                print(
                    "hint: needs BigQuery Connections Admin (or run the SQL from "
                    "'invoice-bq-embeddings create-connection-sql' as an admin). "
                    f"Connection id: {project_id}.{connection_region}.{connection_id}",
                    file=sys.stderr,
                )
                raise
            logger.info(
                "Connection ready: %s.%s.%s — grant its service account "
                "roles/aiplatform.user on the Vertex project if model creation fails "
                "(see bq show --connection).",
                project_id,
                connection_region,
                connection_id,
            )
            model_sql = build_create_remote_embedding_model_ddl(
                project_id=project_id,
                dataset_id=bq_dataset,
                model_id=remote_model,
                connection_region=connection_region,
                connection_id=connection_id,
                endpoint=endpoint,
            )
            try:
                run_ddl(client, model_sql, location=bq_location)
            except Exception:
                logger.exception("CREATE REMOTE MODEL failed")
                print(
                    "hint: grant the connection service account roles/aiplatform.user "
                    "on the project that owns the embedding endpoint, then re-run setup "
                    f"(model `{project_id}.{bq_dataset}.{remote_model}`). "
                    "Inspect SA: bq show --connection "
                    f"{project_id}.{connection_region}.{connection_id}",
                    file=sys.stderr,
                )
                raise
            logger.info(
                "Remote embedding model ready: %s.%s.%s (endpoint %s)",
                project_id,
                bq_dataset,
                remote_model,
                endpoint,
            )

        ref = ensure_invoice_embeddings_table(
            client,
            bq_dataset,
            embeddings_table,
            project_id=project_id,
            location=bq_location,
            embedding_dimensions=out_dim,
        )
        logger.info("Embeddings table ready: %s", ref)

        embed_sql = build_invoice_embed_text_view_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            extractions_table_id=extractions_table,
            view_id=embed_view,
        )
        run_ddl(client, embed_sql, location=bq_location)
        logger.info("View %s.%s.%s created", project_id, bq_dataset, embed_view)

        gl_sql = build_invoice_gl_context_view_ddl(
            project_id=project_id,
            dataset_id=bq_dataset,
            gl_table_id=gl_table,
            view_id=gl_context_view,
        )
        run_ddl(client, gl_sql, location=bq_location)
        logger.info("View %s.%s.%s created", project_id, bq_dataset, gl_context_view)

        if args.with_vector_index:
            idx_sql = build_create_vector_index_ddl(
                project_id=project_id,
                dataset_id=bq_dataset,
                table_id=embeddings_table,
            )
            run_ddl(client, idx_sql, location=bq_location)
            logger.info("Vector index on %s.%s.%s created or already exists", project_id, bq_dataset, embeddings_table)

        logger.info("setup finished")
        return

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

    if args.command == "import-embeddings":
        if not args.jsonl_files:
            print("error: pass one or more NDJSON paths after import-embeddings", file=sys.stderr)
            sys.exit(1)
        table_ref = ensure_invoice_embeddings_table(
            client,
            bq_dataset,
            embeddings_table,
            project_id=project_id,
            location=bq_location,
            embedding_dimensions=out_dim,
        )
        disp = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if args.truncate
            else bigquery.WriteDisposition.WRITE_APPEND
        )
        jobs = load_precomputed_embedding_ndjson_files(
            client,
            table_ref,
            list(args.jsonl_files),
            write_disposition=disp,
            location=bq_location,
        )
        for path, job in zip(args.jsonl_files, jobs, strict=True):
            logger.info(
                "Loaded %s rows from %s (job %s)",
                job.output_rows,
                path,
                job.job_id,
            )
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

    if args.command == "search-stored":
        if not args.gcs_uri:
            print("error: --gcs-uri required", file=sys.stderr)
            sys.exit(1)
        sql = build_vector_search_by_stored_embedding_sql(
            project_id=project_id,
            dataset_id=bq_dataset,
            query_gcs_uri=args.gcs_uri,
            embeddings_table_id=embeddings_table,
            top_k=args.top_k,
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

    if args.command == "rag-search-stored":
        if not args.gcs_uri:
            print("error: --gcs-uri required", file=sys.stderr)
            sys.exit(1)
        sql = build_rag_neighbors_with_gl_stored_embedding_sql(
            project_id=project_id,
            dataset_id=bq_dataset,
            query_gcs_uri=args.gcs_uri,
            embed_text_view_id=embed_view,
            embeddings_table_id=embeddings_table,
            gl_context_view_id=gl_context_view,
            top_k=args.top_k,
        )
        for row in client.query(sql, location=bq_location).result():
            print(dict(row))
        return


if __name__ == "__main__":
    main()
