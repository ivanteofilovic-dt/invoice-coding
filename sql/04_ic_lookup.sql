-- Deterministic IC lookup support. Gemini should copy the resolved IC value,
-- not infer it from invoice text alone.
-- Replace placeholders before execution:
--   {{PROJECT_ID}}, {{DATASET_ID}}

CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{DATASET_ID}}.vendor_ic_mapping` (
  vendor STRING,
  normalized_vendor STRING,
  ic STRING,
  source STRING,
  effective_from DATE,
  effective_to DATE,
  is_active BOOL
);

CREATE OR REPLACE VIEW `{{PROJECT_ID}}.{{DATASET_ID}}.vendor_ic_historical_majority` AS
SELECT
  SUPPLIER_CUSTMER_NAME AS vendor,
  UPPER(TRIM(SUPPLIER_CUSTMER_NAME)) AS normalized_vendor,
  IC,
  COUNT(*) AS usage_count,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (PARTITION BY SUPPLIER_CUSTMER_NAME)
  ) AS usage_share
FROM `{{PROJECT_ID}}.{{DATASET_ID}}.gl_coding_history`
WHERE COALESCE(IC, '') != ''
GROUP BY vendor, normalized_vendor, IC
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY normalized_vendor
  ORDER BY usage_count DESC, IC
) = 1;

-- Runtime lookup for one vendor. Use a threshold of 0.80 before accepting
-- historical majority fallback.
WITH requested_vendor AS (
  SELECT
    'VENDOR_NAME' AS vendor,
    UPPER(TRIM('VENDOR_NAME')) AS normalized_vendor
),
mapping_match AS (
  SELECT
    m.ic,
    'vendor_ic_mapping' AS resolution_source,
    1.0 AS confidence
  FROM requested_vendor AS r
  JOIN `{{PROJECT_ID}}.{{DATASET_ID}}.vendor_ic_mapping` AS m
    ON m.normalized_vendor = r.normalized_vendor
  WHERE m.is_active
    AND CURRENT_DATE() BETWEEN COALESCE(m.effective_from, DATE '1900-01-01')
    AND COALESCE(m.effective_to, DATE '9999-12-31')
  LIMIT 1
),
historical_match AS (
  SELECT
    h.IC AS ic,
    'historical_majority' AS resolution_source,
    h.usage_share AS confidence
  FROM requested_vendor AS r
  JOIN `{{PROJECT_ID}}.{{DATASET_ID}}.vendor_ic_historical_majority` AS h
    ON h.normalized_vendor = r.normalized_vendor
  WHERE h.usage_share >= 0.80
  LIMIT 1
)
SELECT * FROM mapping_match
UNION ALL
SELECT * FROM historical_match
WHERE NOT EXISTS (SELECT 1 FROM mapping_match)
UNION ALL
SELECT
  '' AS ic,
  'default_non_ic' AS resolution_source,
  0.0 AS confidence
WHERE NOT EXISTS (SELECT 1 FROM mapping_match)
  AND NOT EXISTS (SELECT 1 FROM historical_match);
