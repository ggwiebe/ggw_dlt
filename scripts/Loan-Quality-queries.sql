-- Databricks notebook source
----------------------------------------------------------------------------------------
-- DLT Event Log & Data Quality Scores
----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ggw_loans.event_log
 USING delta
LOCATION 'dbfs:/Users/glenn.wiebe@databricks.com/dlt_loans/system/events'
;

SELECT * 
  FROM ggw_loans.event_log
;

-- DESCRIBE TABLE EXTENDED ggw_loans.event_log;

-- COMMAND ----------

----------------------------------------------------------------------------------------
-- Lineage
----------------------------------------------------------------------------------------
SELECT details:flow_definition.output_dataset,
       details:flow_definition.input_datasets,
       details:flow_definition.flow_type,
       details:flow_definition.schema,
       details:flow_definition.explain_text,
       details:flow_definition
  FROM ggw_loans.event_log
 WHERE details:flow_definition IS NOT NULL
 ORDER BY timestamp
;

-- COMMAND ----------

----------------------------------------------------------------------------------------
-- Pipeline Data Components
----------------------------------------------------------------------------------------
SELECT details:flow_definition.output_dataset,
       details:flow_definition.input_datasets
  FROM ggw_loans.event_log
 WHERE details:flow_definition IS NOT NULL
;

-- COMMAND ----------

----------------------------------------------------------------------------------------
-- Flow Progress & Data Quality Results
----------------------------------------------------------------------------------------
SELECT id,
       details:flow_progress.metrics,
       details:flow_progress.data_quality.dropped_records,
       explode(from_json(details:flow_progress:data_quality:expectations
                ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations,
       details:flow_progress
  FROM ggw_loans.event_log
 WHERE details:flow_progress.metrics IS NOT NULL
 ORDER BY timestamp
;

-- COMMAND ----------

----------------------------------------------------------------------------------------
-- Data Quality Expectation Metrics
----------------------------------------------------------------------------------------
SELECT id,
       expectations.dataset,
       expectations.name,
       expectations.failed_records,
       expectations.passed_records
  FROM (
        SELECT id,
               timestamp,
               details:flow_progress.metrics,
               details:flow_progress.data_quality.dropped_records,
               explode(from_json(details:flow_progress:data_quality:expectations
                        ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
          FROM ggw_loans.event_log
         WHERE details:flow_progress.metrics IS NOT NULL
        ) data_quality
;

-- COMMAND ----------

-- Get details from Gold Load Balance Table #2
SELECT location_code,
       bal TotalLoanBalance
  FROM ggw_loans.gl_total_loan_balances_z
 ORDER BY bal DESC
 LIMIT 20
;

