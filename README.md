# Automated Invoice Coding POC

This repository contains a proof of concept for automated invoice coding using:

- BigQuery historical GL preparation in `gl_coding_history`
- BigQuery ML embeddings with `text-multilingual-embedding-002`
- BigQuery `VECTOR_SEARCH` for historical line retrieval
- Gemini extraction and prediction prompts with strict JSON contracts
- Deterministic Intercompany (`IC`) lookup rules

## SQL Assets

The BigQuery scripts live in `sql/` and should be executed in order:

1. `01_create_gl_coding_history.sql`
2. `02_embedding_pipeline.sql`
3. `03_search_and_vendor_context.sql`
4. `04_ic_lookup.sql`
5. `05_prediction_audit_and_evaluation.sql`

Render placeholders using environment variables:

```bash
PROJECT_ID=my-project \
DATASET_ID=invoice_coding \
RAW_GL_TABLE=raw_gl_history \
REGION=europe-west1 \
EMBEDDING_CONNECTION=vertex_ai \
python -m poc_ankrag render-sql --output-dir rendered_sql
```

## Runtime Flow

The Python package in `src/poc_ankrag/` provides:

- prompt builders for Gemini invoice extraction and coding prediction
- fallback detection for generic invoice reference descriptions
- deterministic `IC` resolution
- protocol-based orchestration in `pipeline.py`
- optional Google Cloud clients in `cloud_clients.py`

The runtime expects Google Cloud credentials with BigQuery and Vertex AI access.
