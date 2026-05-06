-- Audit and evaluation storage for automated invoice coding predictions.
-- Replace placeholders before execution:
--   {{PROJECT_ID}}, {{DATASET_ID}}

CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{DATASET_ID}}.invoice_coding_predictions` (
  prediction_id STRING,
  invoice_number STRING,
  vendor STRING,
  invoice_date DATE,
  line_id STRING,
  line_description STRING,
  amount NUMERIC,
  predicted_account STRING,
  predicted_department STRING,
  predicted_product STRING,
  predicted_ic STRING,
  predicted_project STRING,
  predicted_system STRING,
  predicted_reserve STRING,
  confidence FLOAT64,
  reasoning_summary STRING,
  prompt_version STRING,
  extraction_model STRING,
  prediction_model STRING,
  vector_example_row_ids ARRAY<STRING>,
  created_at TIMESTAMP,
  approved_by STRING,
  approved_at TIMESTAMP,
  final_account STRING,
  final_department STRING,
  final_product STRING,
  final_ic STRING,
  final_project STRING,
  final_system STRING,
  final_reserve STRING
);

CREATE OR REPLACE VIEW `{{PROJECT_ID}}.{{DATASET_ID}}.invoice_coding_evaluation` AS
SELECT
  COUNT(*) AS reviewed_lines,
  AVG(CAST(predicted_account = final_account AS INT64)) AS account_accuracy,
  AVG(CAST(predicted_department = final_department AS INT64)) AS department_accuracy,
  AVG(CAST(predicted_product = final_product AS INT64)) AS product_accuracy,
  AVG(CAST(predicted_ic = final_ic AS INT64)) AS ic_accuracy,
  AVG(CAST(predicted_project = final_project AS INT64)) AS project_accuracy,
  AVG(CAST(predicted_system = final_system AS INT64)) AS system_accuracy,
  AVG(CAST(predicted_reserve = final_reserve AS INT64)) AS reserve_accuracy,
  AVG(CAST(
    predicted_account = final_account
    AND predicted_department = final_department
    AND predicted_product = final_product
    AND predicted_ic = final_ic
    AND predicted_project = final_project
    AND predicted_system = final_system
    AND predicted_reserve = final_reserve
  AS INT64)) AS exact_dimension_accuracy
FROM `{{PROJECT_ID}}.{{DATASET_ID}}.invoice_coding_predictions`
WHERE approved_at IS NOT NULL;
