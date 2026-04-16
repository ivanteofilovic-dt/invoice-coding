"""CLI entry for uploading historical GL and invoice files to GCS."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from invoice_processing.gcs_upload import upload_historical_to_gcs


def main() -> None:
    bucket = os.environ.get("GCS_BUCKET")
    if not bucket:
        print("error: set GCS_BUCKET to the target bucket name", file=sys.stderr)
        sys.exit(1)
    data_root = Path(os.environ.get("DATA_ROOT", "data")).expanduser()
    prefix = os.environ.get("GCS_PREFIX", "historical")
    summary = upload_historical_to_gcs(bucket, data_root, prefix=prefix)
    print(
        f"Uploaded gl={summary.gl_uploaded} invoices={summary.invoice_uploaded} "
        f"to gs://{summary.bucket}/ ({summary.prefix or '(no prefix)'})"
    )
    for name in summary.object_names:
        print(f"  - {name}")


if __name__ == "__main__":
    main()
