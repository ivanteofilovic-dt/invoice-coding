-- Creates the BigQuery ML remote embedding model and fills gl_coding_history.embedding.
-- Replace placeholders before execution:
--   {{PROJECT_ID}}, {{DATASET_ID}}, {{REGION}}, {{EMBEDDING_CONNECTION}}

CREATE OR REPLACE MODEL `{{PROJECT_ID}}.{{DATASET_ID}}.gl_embedding_model`
REMOTE WITH CONNECTION `{{PROJECT_ID}}.{{REGION}}.{{EMBEDDING_CONNECTION}}`
OPTIONS (
  ENDPOINT = 'text-multilingual-embedding-002'
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history_embedded` AS
SELECT
  h.* EXCEPT(embedding),
  e.ml_generate_embedding_result AS embedding
FROM `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history` AS h
JOIN ML.GENERATE_EMBEDDING(
  MODEL `{{PROJECT_ID}}.{{DATASET_ID}}.gl_embedding_model`,
  (
    SELECT
      row_id,
      embedding_text AS content
    FROM `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history`
    WHERE TRIM(embedding_text) != ' |  | '
  ),
  STRUCT(TRUE AS flatten_json_output)
) AS e
USING (row_id);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history` AS
SELECT *
FROM `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history_embedded`;

DROP TABLE `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history_embedded`;

CREATE OR REPLACE VECTOR INDEX gl_coding_history_embedding_idx
ON `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history`(embedding)
OPTIONS (
  index_type = 'IVF'
);

SELECT
  COUNT(*) AS total_rows,
  COUNTIF(embedding IS NOT NULL) AS embedded_rows,
  COUNTIF(ARRAY_LENGTH(embedding) > 0) AS non_empty_embeddings
FROM `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history`;
