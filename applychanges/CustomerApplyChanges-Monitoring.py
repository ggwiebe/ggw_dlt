# Databricks notebook source
# MAGIC %md # Delta Live Tables - Monitoring  
# MAGIC   
# MAGIC Each DLT Pipeline stands up its own events table in the Storage Location defined on the pipeline.  
# MAGIC From this table we can see what is happening and the quality of the data passing through it.

# COMMAND ----------

# MAGIC %md ## 0.1 - CONFIG 

# COMMAND ----------

dbutils.widgets.text('root_location', 'abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/')
dbutils.widgets.text('db_name', 'ggw_retail')
dbutils.widgets.text('data_loc','/data')
dbutils.widgets.text('storage_loc','/dlt_storage')
# -- REMOVE WIDGET old

# COMMAND ----------

root_location = dbutils.widgets.get('root_location')
db_name       = dbutils.widgets.get('db_name')
data_loc      = dbutils.widgets.get('data_loc')
storage_loc   = dbutils.widgets.get('storage_loc')
storage_path  = root_location + db_name + storage_loc

print('root_location: {}\ndb_name:       {}\ndata_loc:      {}\nstorage_loc:   {}'.format(root_location,db_name,data_loc,storage_loc))
print('storage_path:  {}'.format(storage_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE $db_name

# COMMAND ----------

# MAGIC %md ## 0.2 - SETUP 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE $db_name.event_log;
# MAGIC CREATE TABLE IF NOT EXISTS $db_name.event_log
# MAGIC  USING delta
# MAGIC LOCATION '$root_location$db_name$storage_loc/system/events'

# COMMAND ----------

# MAGIC %md ## 1 - DLT Events 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC        id,
# MAGIC        timestamp,
# MAGIC        sequence,
# MAGIC        -- origin,
# MAGIC        event_type,
# MAGIC        message,
# MAGIC        level, 
# MAGIC        -- error ,
# MAGIC        details
# MAGIC   FROM $db_name.event_log
# MAGIC  ORDER BY timestamp ASC
# MAGIC ;  

# COMMAND ----------

# MAGIC %md ## 2 - DLT Lineage 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List of output datasets by type and the most recent change
# MAGIC SELECT details:flow_definition.output_dataset output_dataset,
# MAGIC        details:flow_definition.flow_type,
# MAGIC        MAX(timestamp)
# MAGIC   FROM $db_name.event_log
# MAGIC  WHERE details:flow_definition.output_dataset IS NOT NULL
# MAGIC  GROUP BY details:flow_definition.output_dataset,
# MAGIC           details:flow_definition.schema,
# MAGIC           details:flow_definition.flow_type
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC ----------------------------------------------------------------------------------------
# MAGIC -- Lineage
# MAGIC ----------------------------------------------------------------------------------------
# MAGIC SELECT max_timestamp,
# MAGIC        details:flow_definition.output_dataset,
# MAGIC        details:flow_definition.input_datasets,
# MAGIC        details:flow_definition.flow_type,
# MAGIC        details:flow_definition.schema,
# MAGIC        details:flow_definition.explain_text,
# MAGIC        details:flow_definition
# MAGIC   FROM $db_name.event_log e
# MAGIC  INNER JOIN (
# MAGIC               SELECT details:flow_definition.output_dataset output_dataset,
# MAGIC                      MAX(timestamp) max_timestamp
# MAGIC                 FROM $db_name.event_log
# MAGIC                WHERE details:flow_definition.output_dataset IS NOT NULL
# MAGIC                GROUP BY details:flow_definition.output_dataset
# MAGIC             ) m
# MAGIC   WHERE e.timestamp = m.max_timestamp
# MAGIC     AND e.details:flow_definition.output_dataset = m.output_dataset
# MAGIC --    AND e.details:flow_definition IS NOT NULL
# MAGIC  ORDER BY e.details:flow_definition.output_dataset
# MAGIC ;

# COMMAND ----------

# MAGIC %md ## 3 - Quality Metrics 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   id,
# MAGIC   expectations.dataset,
# MAGIC   expectations.name,
# MAGIC   expectations.failed_records,
# MAGIC   expectations.passed_records
# MAGIC FROM(
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     explode(from_json(details:flow_progress:data_quality:expectations
# MAGIC              ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
# MAGIC   FROM event_log
# MAGIC   WHERE details:flow_progress.metrics IS NOT NULL) data_quality

# COMMAND ----------

# MAGIC %md ## 4. Business Aggregate Checks

# COMMAND ----------

# MAGIC %fs cp /Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-2-append.csv /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/customer-99-missing-append.csv

# COMMAND ----------

# MAGIC %fs ls /Users/glenn.wiebe@databricks.com/ggw_retail/data/in

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Get details from Silver merged table
# MAGIC SELECT DISTINCT COUNT(id) RecordCount,
# MAGIC        MAX(id) MaxId,
# MAGIC        MAX(update_dt) MostRecentUpdate
# MAGIC   FROM $db_name.customer_silver
# MAGIC --   ORDER BY bal DESC
# MAGIC  LIMIT 20
# MAGIC ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Get details from Bronze landed table
# MAGIC SELECT COUNT(DISTINCT id) DistinctRecordCount,
# MAGIC        COUNT(1) RecordCount,
# MAGIC        MAX(id) MaxId,
# MAGIC        MAX(update_dt) MostRecentUpdate
# MAGIC   FROM $db_name.customer_bronze
# MAGIC --   ORDER BY bal DESC
# MAGIC  LIMIT 20
# MAGIC ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- -- View is needed to be able to perform join equality on computed value input_file_name()
# MAGIC 
# MAGIC -- Source system (not usually visible to Databricks)
# MAGIC -- CREATE VIEW $db_name.customers_source_inputfiles_v
# MAGIC -- AS
# MAGIC --     SELECT *,
# MAGIC --            input_file_name() AS input_file_name 
# MAGIC --       FROM ggw_retail.customers_source
# MAGIC -- ;
# MAGIC 
# MAGIC -- Input cloudFiles:
# MAGIC -- CREATE OR REPLACE VIEW $db_name.customers_raw_inputfiles_v
# MAGIC -- AS
# MAGIC --     SELECT *,
# MAGIC --            input_file_name() AS input_file_name 
# MAGIC --       FROM ggw_retail.customers_raw
# MAGIC -- ;
# MAGIC 
# MAGIC -- Get files in source not landed in Bronze
# MAGIC -- source input_file_name: dbfs:/Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-1-insert.csv
# MAGIC -- raw    input_file_name: dbfs:/Users/glenn.wiebe@databricks.com/ggw_retail/data/in/customer-1-insert.csv
# MAGIC -- landed input_file_name: dbfs:/Users/glenn.wiebe@databricks.com/ggw_retail/data/in/customer-1-insert.csv
# MAGIC 
# MAGIC SELECT *
# MAGIC   FROM $db_name.customers_raw_inputfiles_v
# MAGIC  WHERE input_file_name NOT IN (
# MAGIC                                 SELECT DISTINCT ('dbfs:' || input_file_name) distinct_input_file_name
# MAGIC                                            FROM $db_name.customer_bronze
# MAGIC                                           ORDER BY distinct_input_file_name
# MAGIC                               )
# MAGIC  ORDER BY update_dt, id ASC
# MAGIC ;

# COMMAND ----------


