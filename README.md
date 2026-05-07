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

The runtime expects Google Cloud Application Default Credentials or service account credentials with BigQuery and Vertex AI access. Gemini calls are made through Vertex AI, not an API key.

## Predict Coding From A PDF

After the SQL assets have been created and Google Cloud credentials are available, run:

```bash
PROJECT_ID=my-project \
DATASET_ID=invoice_coding \
REGION=europe-west1 \
python -m poc_ankrag predict-pdf path/to/invoice.pdf --pretty
```

The command sends the PDF to Gemini for structured invoice extraction, retrieves historical GL evidence from BigQuery, predicts coding dimensions for each invoice line, writes prediction audit rows, and returns JSON. The `estimated_accuracy` field is the model confidence score returned by Gemini; measured accuracy requires comparing predictions against known approved coding.

For local development, authenticate with:

```bash
gcloud auth application-default login
gcloud config set project my-project
```

## Web Application

The POC also includes a FastAPI backend and a React frontend based on the original `frontend.js`
canvas prototype.

Install Python dependencies and run the API:

```bash
pip install -e .
PROJECT_ID=my-project \
DATASET_ID=invoice_coding \
REGION=europe-west1 \
uvicorn poc_ankrag.api:app --reload
```

The API exposes:

- `GET /api/health` for a lightweight health check
- `POST /api/invoices/code` for PDF invoice upload and coding
- `POST /api/invoices/batch/code` for multi-PDF upload with local bounded parallel processing

Batch processing does not require extra cloud resources. The API processes uploaded PDFs in
parallel inside the FastAPI process, keeps successful invoices even when individual files fail,
and returns per-file errors. Set `BATCH_CONCURRENCY` to control the local worker count; the
default is `4`.

In another terminal, install and run the frontend:

```bash
cd frontend
npm install
npm run dev
```

By default Vite proxies `/api` requests to `http://localhost:8000`. To point the frontend at a
different backend URL, set `VITE_API_BASE_URL`.
