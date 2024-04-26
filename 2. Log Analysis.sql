-- Databricks notebook source
-- MAGIC %md # DLT Event Log

-- COMMAND ----------

-- MAGIC %md ## 0. Setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text('storage_location', 'dbfs:/Users/glenn.wiebe@databricks.com/dlt_loans', 'Storage Location')
-- MAGIC dbutils.widgets.text('db', 'ggw_loans', 'DB Name')

-- COMMAND ----------

-- MAGIC %python display(dbutils.fs.ls(dbutils.widgets.get('storage_location')))

-- COMMAND ----------

USE ${db}

-- COMMAND ----------

-- CREATE OR REPLACE VIEW loan_pipeline_logs
-- AS SELECT * FROM delta.`dbfs:/Users/glenn.wiebe@databricks.com/dlt_loans/system/events`

CREATE TABLE IF NOT EXISTS ggw_loans.event_log
USING delta
LOCATION 'dbfs:/Users/glenn.wiebe@databricks.com/dlt_loans/system/events'


-- COMMAND ----------

-- MAGIC %md ## 1. Event Log - Raw Sequence of Events by Timestamp

-- COMMAND ----------

SELECT * FROM event_log
ORDER BY timestamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
-- MAGIC * `user_action` Events occur when taking actions like creating the pipeline
-- MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
-- MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
-- MAGIC   * `flow_type` - whether this is a complete or append flow
-- MAGIC   * `explain_text` - the Spark explain plan
-- MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
-- MAGIC   * `metrics` - currently contains `num_output_rows`
-- MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
-- MAGIC     * `dropped_records`
-- MAGIC     * `expectations`
-- MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md ## DLT Quality - Flow Events

-- COMMAND ----------

SELECT
  details:flow_definition.output_dataset,
  details:flow_definition.input_datasets,
  details:flow_definition.flow_type,
  details:flow_definition.schema,
  details:flow_definition
FROM event_log
WHERE details:flow_definition IS NOT NULL
ORDER BY timestamp

-- COMMAND ----------

-- MAGIC %md ## DLT Quality - Pass/Fail Metrics

-- COMMAND ----------

SELECT
  id,
  expectations.dataset,
  expectations.name,
  expectations.failed_records,
  expectations.passed_records
FROM(
  SELECT 
    id,
    timestamp,
    details:flow_progress.metrics,
    details:flow_progress.data_quality.dropped_records,
    explode(from_json(details:flow_progress:data_quality:expectations
             ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
  FROM event_log
  WHERE details:flow_progress.metrics IS NOT NULL) data_quality


-- COMMAND ----------


