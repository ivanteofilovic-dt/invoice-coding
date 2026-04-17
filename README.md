# invoice-processing

Pipeline utilities for invoice and GL data processing.

## Historical upload to GCS

Set `GCS_BUCKET`, optionally `DATA_ROOT` (default `data`) and `GCS_PREFIX` (default `historical`), then run:

```bash
python -m invoice_processing.cli
```

Or use the console script `invoice-upload-historical` after install.

## RAG coding API and UI

Prerequisites:

- **GCP**: `GCP_PROJECT`, `BQ_DATASET`, `VERTEX_LOCATION` (e.g. `us-central1`).
- **GCS** (`GCS_BUCKET`): optional for **upload/analyze**. If unset or empty, the API sends the PDF to Vertex Gemini as **inline bytes** (no object storage). Rows in BigQuery still use a logical key like `inline://{run_id}/{filename}` in the `gcs_uri` column so RAG SQL works. For **batch historical extraction** (`invoice-batch-extract`) and uploads to GCS, you still need a bucket when you use those commands.
- **BigQuery**: `invoice_extractions`, `invoice_embeddings`, views `v_invoice_embed_text` and `v_invoice_gl_context`, remote embedding model (see `invoice-bq-embeddings setup` in [`pyproject.toml`](pyproject.toml)).
- **Optional env**: `VERTEX_GEMINI_MODEL` (default `gemini-2.5-flash`), `GCS_NEW_INVOICE_PREFIX` (default `new_invoices`), `RAG_TOP_K` (default `10`), `BQ_RAG_SUGGESTIONS_TABLE` (default `rag_suggestions`), `CONFIDENCE_HIGH_THRESHOLD` / `CONFIDENCE_LOW_THRESHOLD` (for future tuning; status rules are in code).

Run the API (port 8000):

```bash
pip install -e .
ankrag serve
```

Run the example React UI (from another terminal; proxies `/api` to the API):

```bash
cd src/frontend && npm install && npm run dev
```

If you see **`ERR_MODULE_NOT_FOUND`** for **`cli.js`** (Vite), the install is incomplete: remove `src/frontend/node_modules` and run `npm install` again inside `src/frontend`. A repo-wide `.gitignore` rule `dist/` used to ignore **`node_modules/vite/dist/`**, which breaks Vite; use a fresh install after pulling the fixed `.gitignore`.

Headless analyze on a local PDF (prints JSON; add `--persist` to store a row in `rag_suggestions`):

```bash
ankrag suggest --local-pdf path/to/invoice.pdf
```

Flow: upload PDF to GCS → Vertex Gemini extracts JSON → row appended to `invoice_extractions` → BigQuery `VECTOR_SEARCH` on historical `invoice_embeddings` with GL join → second Gemini call proposes journal lines and confidence → optional persist in `rag_suggestions`.
