"""Optional Google Cloud client implementations for the orchestration protocols."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from poc_ankrag.config import PipelineConfig
from poc_ankrag.ic_resolver import HistoricalICUsage, VendorICMapping
from poc_ankrag.matching import build_embedding_content
from poc_ankrag.models import HistoricalExample, VendorCodingSummary
from poc_ankrag.pipeline import PredictionRecord, SimilarLineSearch


class GeminiJSONClient:
    """Gemini JSON client backed by Vertex AI and Google Cloud credentials."""

    def __init__(self, config: PipelineConfig) -> None:
        try:
            from google import genai
        except ImportError as exc:
            raise RuntimeError("Install google-genai to use GeminiJSONClient") from exc
        self._client = genai.Client(
            vertexai=True,
            project=config.project_id,
            location=config.region,
        )

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

    def search_similar_lines_batch(
        self,
        searches: list[SimilarLineSearch],
        *,
        top_k: int = 20,
    ) -> dict[str, list[HistoricalExample]]:
        if not searches:
            return {}

        input_rows_sql = "\nUNION ALL\n".join(
            f"""
          SELECT
            @line_id_{index} AS line_id,
            @vendor_{index} AS vendor,
            @line_description_{index} AS line_description,
            @vendor_only_{index} AS use_vendor_only_matching,
            @embedding_content_{index} AS content
            """
            for index, _ in enumerate(searches)
        )
        query = f"""
        WITH invoice_line_query AS (
          {input_rows_sql}
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
        ORDER BY query.line_id, distance ASC
        """
        params: dict[str, Any] = {"top_k": top_k}
        for index, search in enumerate(searches):
            params.update(
                {
                    f"line_id_{index}": search.line_id,
                    f"vendor_{index}": search.vendor,
                    f"line_description_{index}": search.line_description,
                    f"vendor_only_{index}": search.vendor_only,
                    f"embedding_content_{index}": search.embedding_content,
                }
            )

        results = {search.line_id: [] for search in searches}
        for row in self._client.query(query, job_config=self._job_config(**params)):
            results.setdefault(row.line_id, []).append(self._historical_example_from_row(row))
        return results

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
        self.save_predictions([prediction])

    def save_predictions(self, predictions: list[PredictionRecord]) -> None:
        if not predictions:
            return

        table_id = f"{self._config.project_id}.{self._config.dataset_id}.invoice_coding_predictions"
        created_at = datetime.now(timezone.utc)
        prediction_ids = [str(uuid4()) for _ in predictions]
        rows = [
            self._prediction_row(prediction, prediction_id, created_at)
            for prediction, prediction_id in zip(predictions, prediction_ids, strict=True)
        ]
        errors = self._client.insert_rows_json(table_id, rows)
        if errors:
            raise RuntimeError(f"Failed to insert prediction audit rows: {errors}")
        self._append_predictions_to_history(predictions, prediction_ids)

    def _append_predictions_to_history(
        self,
        predictions: list[PredictionRecord],
        prediction_ids: list[str],
    ) -> None:
        input_rows_sql = "\nUNION ALL\n".join(
            f"""
          SELECT
            @history_row_id_{index} AS row_id,
            CAST(NULL AS STRING) AS ENTITY,
            'AUTOMATED_INVOICE_CODING' AS GL_SOURCE_NAME,
            'Purchase Invoices' AS GL_CATEGORY,
            @invoice_number_{index} AS JOURNAL_NUMBER,
            SAFE_CAST(@invoice_date_{index} AS DATE) AS POSTING_DATE,
            FORMAT_DATE('%Y-%m', SAFE_CAST(@invoice_date_{index} AS DATE)) AS PERIOD,
            @account_{index} AS ACCOUNT,
            @account_{index} AS HFM_ACCOUNT,
            @reasoning_summary_{index} AS HFM_DSCRIPTIONS,
            @department_{index} AS DEPARTMENT,
            @product_{index} AS PRODUCT,
            CAST(NULL AS STRING) AS WORK_ORDER,
            @ic_{index} AS IC,
            @project_{index} AS PROJECT,
            @system_{index} AS SYSTEM,
            @reserve_{index} AS RESERVE,
            @invoice_number_{index} AS INVOICE_NUM,
            CAST(NULL AS STRING) AS SUPPLIER_NUMBER,
            @vendor_{index} AS SUPPLIER_CUSTMER_NAME,
            @line_description_{index} AS GL_LINE_DESCRIPTION,
            CAST(NULL AS STRING) AS PO_NUMBER,
            SAFE_CAST(@amount_{index} AS NUMERIC) AS AMOUNT,
            'Automated Invoice Coding' AS TRANSACTION_TYPE_NAME,
            CAST(NULL AS STRING) AS GL_TAX,
            CAST(NULL AS STRING) AS SUBLEDGER_TAX_CODE,
            CAST(NULL AS STRING) AS EMPLOYEE_NAME,
            @embedding_text_{index} AS embedding_text,
            @embedding_text_{index} AS content
            """
            for index, _ in enumerate(predictions)
        )
        query = f"""
        INSERT INTO `{self._config.project_id}.{self._config.dataset_id}.gl_coding_history` (
          row_id,
          ENTITY,
          GL_SOURCE_NAME,
          GL_CATEGORY,
          JOURNAL_NUMBER,
          POSTING_DATE,
          PERIOD,
          ACCOUNT,
          HFM_ACCOUNT,
          HFM_DSCRIPTIONS,
          DEPARTMENT,
          PRODUCT,
          WORK_ORDER,
          IC,
          PROJECT,
          SYSTEM,
          RESERVE,
          INVOICE_NUM,
          SUPPLIER_NUMBER,
          SUPPLIER_CUSTMER_NAME,
          GL_LINE_DESCRIPTION,
          PO_NUMBER,
          AMOUNT,
          TRANSACTION_TYPE_NAME,
          GL_TAX,
          SUBLEDGER_TAX_CODE,
          EMPLOYEE_NAME,
          embedding_text,
          embedding
        )
        WITH prediction_history AS (
          {input_rows_sql}
        )
        SELECT
          h.* EXCEPT(content),
          e.ml_generate_embedding_result AS embedding
        FROM prediction_history AS h
        JOIN ML.GENERATE_EMBEDDING(
          MODEL `{self._config.project_id}.{self._config.dataset_id}.gl_embedding_model`,
          (SELECT row_id, content FROM prediction_history),
          STRUCT(TRUE AS flatten_json_output)
        ) AS e
        USING (row_id)
        """
        params: dict[str, Any] = {}
        for index, prediction in enumerate(predictions):
            line = next(item for item in prediction.invoice.lines if item.line_id == prediction.line_id)
            params.update(
                {
                    f"history_row_id_{index}": f"prediction:{prediction_ids[index]}",
                    f"invoice_number_{index}": prediction.invoice.invoice_number,
                    f"invoice_date_{index}": prediction.invoice.invoice_date.isoformat()
                    if prediction.invoice.invoice_date
                    else None,
                    f"account_{index}": prediction.prediction.account,
                    f"reasoning_summary_{index}": prediction.prediction.reasoning_summary,
                    f"department_{index}": prediction.prediction.department,
                    f"product_{index}": prediction.prediction.product,
                    f"ic_{index}": prediction.prediction.ic,
                    f"project_{index}": prediction.prediction.project,
                    f"system_{index}": prediction.prediction.system,
                    f"reserve_{index}": prediction.prediction.reserve,
                    f"vendor_{index}": prediction.invoice.vendor,
                    f"line_description_{index}": line.description,
                    f"amount_{index}": str(line.amount),
                    f"embedding_text_{index}": build_embedding_content(
                        prediction.invoice.vendor,
                        line.description,
                    ),
                }
            )
        self._client.query(query, job_config=self._job_config(**params)).result()

    @staticmethod
    def _prediction_row(
        prediction: PredictionRecord,
        prediction_id: str,
        created_at: datetime,
    ) -> dict[str, Any]:
        line = next(item for item in prediction.invoice.lines if item.line_id == prediction.line_id)
        return {
            "prediction_id": prediction_id,
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
            "created_at": created_at.isoformat(),
        }

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
