-- Databricks notebook source
-- MAGIC %md # Setup Database & File System for Pipeline
-- MAGIC   
-- MAGIC ![Notebook Ingestion Job Flow](https://github.com/ggwiebe/db-fe-dlt/raw/main/dlt/Ingest2Egress/Images/DLT-Jobs.png)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("db_name", "ggw_ods")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC db_name = dbutils.widgets.get("db_name")
-- MAGIC print("Using database name: {}".format(db_name))

-- COMMAND ----------

-- MAGIC %md ## Create DB
-- MAGIC   
-- MAGIC Use ADLS Location

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS $db_name
       LOCATION 'abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/${db_name}/${db_name}.db'

-- COMMAND ----------

use ${db_name};

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED $db_name

-- COMMAND ----------

-- MAGIC %md ## Check tables  
-- MAGIC   
-- MAGIC After Ingest2Table, we should see a table in the list 

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md ## Add DLT Monitoring Event Log table
-- MAGIC   
-- MAGIC After running the DLT pipeline you can run the below CREATE TABLE command to register the Event Log table

-- COMMAND ----------

DROP TABLE $db_name.event_log;
CREATE TABLE IF NOT EXISTS $db_name.event_log
 USING DELTA
 LOCATION 'abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/${db_name}/dlt_storage/system/events'
;
SELECT "Creating ${db_name}.event_log";

-- COMMAND ----------

-- Events
SELECT 
       timestamp,
       event_type,
       message,
       level, 
       -- error ,
       details
       -- id,
       -- origin,
       -- sequence
  FROM $db_name.event_log
 ORDER BY timestamp ASC
;  

-- COMMAND ----------

-- List of Datasets by type
SELECT details:flow_definition.output_dataset output_dataset,
       details:flow_definition.flow_type,
       MAX(timestamp)
  FROM $db_name.event_log
 WHERE details:flow_definition.output_dataset IS NOT NULL
 GROUP BY details:flow_definition.output_dataset,
          details:flow_definition.schema,
          details:flow_definition.flow_type
;

-- COMMAND ----------

-- Lineage
SELECT max_timestamp,
       details:flow_definition.output_dataset,
       details:flow_definition.input_datasets,
       details:flow_definition.flow_type,
       details:flow_definition.schema,
       details:flow_definition.explain_text,
       details:flow_definition
  FROM $db_name.event_log e
 INNER JOIN (
              SELECT details:flow_definition.output_dataset output_dataset,
                     MAX(timestamp) max_timestamp
                FROM $db_name.event_log
               WHERE details:flow_definition.output_dataset IS NOT NULL
               GROUP BY details:flow_definition.output_dataset
            ) m
  WHERE e.timestamp = m.max_timestamp
    AND e.details:flow_definition.output_dataset = m.output_dataset
--    AND e.details:flow_definition IS NOT NULL
 ORDER BY e.details:flow_definition.output_dataset
;

-- COMMAND ----------

-- Quality Metrics
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
;

-- COMMAND ----------

-- MAGIC %md ## Create File System Structure

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/{}/data/in'.format(db_name))
-- MAGIC dbutils.fs.mkdirs('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/{}/data/out'.format(db_name))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/{}'.format(db_name))
