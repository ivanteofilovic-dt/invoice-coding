"""Configuration helpers for BigQuery and Gemini resources."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    project_id: str
    dataset_id: str
    raw_gl_table: str
    region: str = "europe-west1"
    embedding_connection: str = "vertex_ai"
    extraction_model: str = "gemini-2.5-pro"
    prediction_model: str = "gemini-2.5-pro"
    prompt_version: str = "invoice-coding-v1"

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        return cls(
            project_id=os.environ["PROJECT_ID"],
            dataset_id=os.environ["DATASET_ID"],
            raw_gl_table=os.environ.get("RAW_GL_TABLE", "raw_gl_history"),
            region=os.environ.get("REGION", "europe-west1"),
            embedding_connection=os.environ.get("EMBEDDING_CONNECTION", "vertex_ai"),
            extraction_model=os.environ.get("EXTRACTION_MODEL", "gemini-2.5-pro"),
            prediction_model=os.environ.get("PREDICTION_MODEL", "gemini-2.5-pro"),
            prompt_version=os.environ.get("PROMPT_VERSION", "invoice-coding-v1"),
        )

    def sql_replacements(self) -> dict[str, str]:
        return {
            "PROJECT_ID": self.project_id,
            "DATASET_ID": self.dataset_id,
            "RAW_GL_TABLE": self.raw_gl_table,
            "REGION": self.region,
            "EMBEDDING_CONNECTION": self.embedding_connection,
        }
