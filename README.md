# invoice-processing

Pipeline utilities for invoice and GL data processing.

## Historical upload to GCS

Set `GCS_BUCKET`, optionally `DATA_ROOT` (default `data`) and `GCS_PREFIX` (default `historical`), then run:

```bash
python -m invoice_processing.cli
```

Or use the console script `invoice-upload-historical` after install.
