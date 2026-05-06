"""Optional Google Cloud client implementations for the orchestration protocols."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from poc_ankrag.config import PipelineConfig
from poc_ankrag.ic_resolver import HistoricalICUsage, VendorICMapping
from poc_ankrag.models import HistoricalExample, VendorCodingSummary
from poc_ankrag.pipeline import PredictionRecord


class GeminiJSONClient:
    """Gemini JSON client backed by the Google Gen AI SDK."""

    def __init__(self) -> None:
        try:
            from google import genai
        except ImportError as exc:
            raise RuntimeError("Install google-genai to use GeminiJSONClient") from exc
        self._client = genai.Client()

    def generate_json(self, prompt: str, *, model: str) -> dict:
        response = self._client.models.generate_content(
            model=model,
            contents=prompt,
            config={"response_mime_type": "application/json"},
        )
        return json.loads(response.text or "{}")

    def generate_json_from_pdf(
        self,
        prompt: str,
        *,
        pdf_bytes: bytes,
        model: str,
        mime_type: str = "application/pdf",
    ) -> dict:
        try:
            from google.genai import types
        except ImportError as exc:
            raise RuntimeError("Install google-genai to use GeminiJSONClient") from exc

        response = self._client.models.generate_content(
            model=model,
            contents=[
                prompt,
                types.Part.from_bytes(data=pdf_bytes, mime_type=mime_type),
            ],
            config={"response_mime_type": "application/json"},
        )
        return json.loads(response.text or "{}")


class BigQueryCodingHistoryStore:
    """BigQuery implementation of invoice coding retrieval and persistence."""

    def __init__(self, config: PipelineConfig) -> None:
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise RuntimeError("Install google-cloud-bigquery to use BigQueryCodingHistoryStore") from exc

        self._bigquery = bigquery
        self._client = bigquery.Client(project=config.project_id)
        self._config = config

    def search_similar_lines(
        self,
        *,
        line_id: str,
        vendor: str,
        line_description: str,
        embedding_content: str,
        vendor_only: bool,
        top_k: int = 20,
    ) -> list[HistoricalExample]:
        query = f"""
        WITH invoice_line_query AS (
          SELECT
            @line_id AS line_id,
            @vendor AS vendor,
            @line_description AS line_description,
            @vendor_only AS use_vendor_only_matching,
            @embedding_content AS content
        ),
        invoice_line_embedding AS (
          SELECT
            line_id,
            vendor,
            line_description,
            use_vendor_only_matching,
            ml_generate_embedding_result AS embedding
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{self._config.project_id}.{self._config.dataset_id}.gl_embedding_model`,
            (SELECT * FROM invoice_line_query),
            STRUCT(TRUE AS flatten_json_output)
          )
        )
        SELECT
          query.line_id,
          base.row_id AS historical_row_id,
          base.SUPPLIER_CUSTMER_NAME,
          base.GL_LINE_DESCRIPTION,
          base.HFM_DSCRIPTIONS,
          base.ACCOUNT,
          base.DEPARTMENT,
          base.PRODUCT,
          base.IC,
          base.PROJECT,
          base.SYSTEM,
          base.RESERVE,
          base.AMOUNT,
          base.POSTING_DATE,
          distance
        FROM VECTOR_SEARCH(
          TABLE `{self._config.project_id}.{self._config.dataset_id}.gl_coding_history`,
          'embedding',
          (SELECT * FROM invoice_line_embedding),
          'embedding',
          top_k => @top_k,
          distance_type => 'COSINE'
        )
        WHERE
          NOT query.use_vendor_only_matching
          OR UPPER(base.SUPPLIER_CUSTMER_NAME) = UPPER(query.vendor)
        ORDER BY distance ASC
        """
        job_config = self._job_config(
            line_id=line_id,
            vendor=vendor,
            line_description=line_description,
            embedding_content=embedding_content,
            vendor_only=vendor_only,
            top_k=top_k,
        )
        return [self._historical_example_from_row(row) for row in self._client.query(query, job_config=job_config)]

    def fetch_vendor_summary(self, vendor: str, *, limit: int = 50) -> list[VendorCodingSummary]:
        query = f"""
        SELECT
          vendor,
          ACCOUNT,
          DEPARTMENT,
          PRODUCT,
          SYSTEM,
          RESERVE,
          usage_count,
          vendor_usage_share,
          total_abs_amount,
          avg_abs_amount,
          most_recent_posting_date
        FROM `{self._config.project_id}.{self._config.dataset_id}.vendor_coding_summary`
        WHERE UPPER(vendor) = UPPER(@vendor)
        ORDER BY usage_count DESC, most_recent_posting_date DESC
        LIMIT @limit
        """
        rows = self._client.query(query, job_config=self._job_config(vendor=vendor, limit=limit))
        return [self._vendor_summary_from_row(row) for row in rows]

    def fetch_vendor_ic_mappings(self, vendor: str) -> list[VendorICMapping]:
        query = f"""
        SELECT vendor, ic, source, effective_from, effective_to, is_active
        FROM `{self._config.project_id}.{self._config.dataset_id}.vendor_ic_mapping`
        WHERE normalized_vendor = UPPER(TRIM(@vendor))
          AND is_active
        """
        rows = self._client.query(query, job_config=self._job_config(vendor=vendor))
        return [
            VendorICMapping(
                vendor=row.vendor,
                ic=row.ic,
                source=row.source,
                effective_from=row.effective_from,
                effective_to=row.effective_to,
                is_active=row.is_active,
            )
            for row in rows
        ]

    def fetch_historical_ic_usage(self, vendor: str) -> list[HistoricalICUsage]:
        query = f"""
        SELECT vendor, IC, usage_count, usage_share
        FROM `{self._config.project_id}.{self._config.dataset_id}.vendor_ic_historical_majority`
        WHERE normalized_vendor = UPPER(TRIM(@vendor))
        """
        rows = self._client.query(query, job_config=self._job_config(vendor=vendor))
        return [
            HistoricalICUsage(
                vendor=row.vendor,
                ic=row.IC,
                usage_count=row.usage_count,
                usage_share=float(row.usage_share),
            )
            for row in rows
        ]

    def save_prediction(self, prediction: PredictionRecord) -> None:
        table_id = f"{self._config.project_id}.{self._config.dataset_id}.invoice_coding_predictions"
        line = next(item for item in prediction.invoice.lines if item.line_id == prediction.line_id)
        row = {
            "prediction_id": str(uuid4()),
            "invoice_number": prediction.invoice.invoice_number,
            "vendor": prediction.invoice.vendor,
            "invoice_date": prediction.invoice.invoice_date.isoformat()
            if prediction.invoice.invoice_date
            else None,
            "line_id": line.line_id,
            "line_description": line.description,
            "amount": str(line.amount),
            "predicted_account": prediction.prediction.account,
            "predicted_department": prediction.prediction.department,
            "predicted_product": prediction.prediction.product,
            "predicted_ic": prediction.prediction.ic,
            "predicted_project": prediction.prediction.project,
            "predicted_system": prediction.prediction.system,
            "predicted_reserve": prediction.prediction.reserve,
            "confidence": prediction.prediction.confidence,
            "reasoning_summary": prediction.prediction.reasoning_summary,
            "prompt_version": prediction.prompt_version,
            "extraction_model": prediction.extraction_model,
            "prediction_model": prediction.prediction_model,
            "vector_example_row_ids": prediction.vector_example_row_ids,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        errors = self._client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"Failed to insert prediction audit row: {errors}")

    def _job_config(self, **params: Any) -> Any:
        query_parameters = []
        for name, value in params.items():
            parameter_type = "STRING"
            if isinstance(value, bool):
                parameter_type = "BOOL"
            elif isinstance(value, int):
                parameter_type = "INT64"
            query_parameters.append(self._bigquery.ScalarQueryParameter(name, parameter_type, value))
        return self._bigquery.QueryJobConfig(query_parameters=query_parameters)

    @staticmethod
    def _historical_example_from_row(row: Any) -> HistoricalExample:
        return HistoricalExample(
            historical_row_id=row.historical_row_id,
            supplier_customer_name=row.SUPPLIER_CUSTMER_NAME,
            gl_line_description=row.GL_LINE_DESCRIPTION,
            hfm_descriptions=row.HFM_DSCRIPTIONS,
            account=row.ACCOUNT,
            department=row.DEPARTMENT,
            product=row.PRODUCT,
            ic=row.IC,
            project=row.PROJECT,
            system=row.SYSTEM,
            reserve=row.RESERVE,
            amount=Decimal(str(row.AMOUNT)) if row.AMOUNT is not None else None,
            posting_date=row.POSTING_DATE,
            distance=float(row.distance),
        )

    @staticmethod
    def _vendor_summary_from_row(row: Any) -> VendorCodingSummary:
        return VendorCodingSummary(
            vendor=row.vendor,
            account=row.ACCOUNT,
            department=row.DEPARTMENT,
            product=row.PRODUCT,
            system=row.SYSTEM,
            reserve=row.RESERVE,
            usage_count=row.usage_count,
            vendor_usage_share=float(row.vendor_usage_share),
            total_abs_amount=Decimal(str(row.total_abs_amount))
            if row.total_abs_amount is not None
            else None,
            avg_abs_amount=Decimal(str(row.avg_abs_amount)) if row.avg_abs_amount is not None else None,
            most_recent_posting_date=row.most_recent_posting_date,
        )
