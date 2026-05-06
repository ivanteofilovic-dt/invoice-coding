-- Runtime SQL for embedding new invoice lines, finding similar GL history,
-- and collecting vendor-level statistical coding context.
-- Replace placeholders before execution:
--   {{PROJECT_ID}}, {{DATASET_ID}}

CREATE OR REPLACE VIEW `{{PROJECT_ID}}.{{DATASET_ID}}.vendor_coding_summary` AS
SELECT
  SUPPLIER_CUSTMER_NAME AS vendor,
  ACCOUNT,
  DEPARTMENT,
  PRODUCT,
  SYSTEM,
  RESERVE,
  COUNT(*) AS usage_count,
  ROUND(
    SAFE_DIVIDE(
      COUNT(*),
      SUM(COUNT(*)) OVER (PARTITION BY SUPPLIER_CUSTMER_NAME)
    ),
    4
  ) AS vendor_usage_share,
  SUM(ABS(AMOUNT)) AS total_abs_amount,
  AVG(ABS(AMOUNT)) AS avg_abs_amount,
  MAX(POSTING_DATE) AS most_recent_posting_date
FROM `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history`
GROUP BY vendor, ACCOUNT, DEPARTMENT, PRODUCT, SYSTEM, RESERVE;

-- Replace this TEMP TABLE with invoice lines emitted by Gemini extraction.
CREATE TEMP TABLE invoice_lines_input AS
SELECT
  'line-001' AS line_id,
  'VENDOR_NAME' AS vendor,
  'LINE_DESCRIPTION' AS line_description,
  FALSE AS use_vendor_only_matching;

CREATE TEMP TABLE invoice_line_query AS
SELECT
  line_id,
  vendor,
  line_description,
  use_vendor_only_matching,
  CASE
    WHEN use_vendor_only_matching THEN CONCAT(vendor, ' | ', '', ' | ', '')
    ELSE CONCAT(vendor, ' | ', line_description, ' | ', line_description)
  END AS content
FROM invoice_lines_input;

CREATE TEMP TABLE invoice_line_embedding AS
SELECT
  line_id,
  vendor,
  line_description,
  use_vendor_only_matching,
  ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(
  MODEL `{{PROJECT_ID}}.{{DATASET_ID}}.gl_embedding_model`,
  TABLE invoice_line_query,
  STRUCT(TRUE AS flatten_json_output)
);

SELECT
  query.line_id,
  query.vendor AS query_vendor,
  query.line_description AS query_line_description,
  query.use_vendor_only_matching,
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
  TABLE `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history`,
  'embedding',
  TABLE invoice_line_embedding,
  'embedding',
  top_k => 20,
  distance_type => 'COSINE'
)
WHERE
  NOT query.use_vendor_only_matching
  OR UPPER(base.SUPPLIER_CUSTMER_NAME) = UPPER(query.vendor)
ORDER BY query.line_id, distance ASC;

-- Vendor summary lookup for the same extracted invoice lines.
SELECT
  l.line_id,
  s.vendor,
  s.ACCOUNT,
  s.DEPARTMENT,
  s.PRODUCT,
  s.SYSTEM,
  s.RESERVE,
  s.usage_count,
  s.vendor_usage_share,
  s.total_abs_amount,
  s.avg_abs_amount,
  s.most_recent_posting_date
FROM invoice_lines_input AS l
JOIN `{{PROJECT_ID}}.{{DATASET_ID}}.vendor_coding_summary` AS s
  ON UPPER(s.vendor) = UPPER(l.vendor)
ORDER BY l.line_id, s.usage_count DESC, s.most_recent_posting_date DESC
LIMIT 50;
