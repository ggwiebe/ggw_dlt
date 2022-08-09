-- Databricks notebook source
-- MAGIC %md ## Delta Live Table - Retail Reference Data DLT Load
-- MAGIC   
-- MAGIC ![DLT Process Flow](https://raw.githubusercontent.com/ggwiebe/db-fe-dlt/main/dlt/applychanges/images/DLT_Process_Flow.png)

-- COMMAND ----------

-- MAGIC %md ### 1. Silver/Reference - Land Raw Data and standardize types
-- MAGIC   
-- MAGIC **Common Storage Format:** Delta  
-- MAGIC **Data Types:** Cast & check Nulls

-- COMMAND ----------

-- BRONZE - Read raw streaming file reader for "new" full Channel reference data
CREATE STREAMING LIVE TABLE channel
  (
    channelId int                 COMMENT 'ID of Sales Channel casted to int',
    channelName string            COMMENT 'Name of Retail Sales Channel',
    description string            COMMENT 'Description of Retail Sales Channel',
    input_file_name string        COMMENT 'Name of file in raw storage bucket',
    dlt_ingest_dt timestamp       COMMENT 'timestamp of dlt ingest', 
    dlt_ingest_procedure string   COMMENT 'name of the routine used to load table',
    dlt_ingest_principal string   COMMENT 'name of principal running load routine',
    CONSTRAINT valid_channel_id   EXPECT (channelId IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_channel_name EXPECT (channelName IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
TBLPROPERTIES ("quality" = "reference")
COMMENT "Latest Channel Reference dataset ingested from cloud object storage landing zone"
AS 
SELECT 
    CAST(channelId  AS int),
    channelName,
    description,
    input_file_name() input_file_name,
    current_timestamp() dlt_ingest_dt,
    "RetailReference_Live" dlt_ingest_procedure,
    current_user() dlt_ingest_principal
  FROM cloud_files('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/in_reference/', 'csv')
--                    ,map('header', 'true', 'schema', 'channelId int, channelName string, description string'))
