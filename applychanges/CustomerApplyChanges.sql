-- Databricks notebook source
-- MAGIC %md ## Delta Live Table - Customer Change Data Capture Demo. 
-- MAGIC   
-- MAGIC ![DLT Process Flow](https://raw.githubusercontent.com/ggwiebe/db-fe-dlt/main/dlt/applychanges/images/DLT_Process_Flow.png)

-- COMMAND ----------

-- MAGIC %md ### 0. Raw - Access Stream  
-- MAGIC   
-- MAGIC **Common Storage Format:** CloudFiles, Kafka, (non-DLT) Delta tables, etc.  
-- MAGIC 
-- MAGIC Here is an example against a Delta table:
-- MAGIC ```
-- MAGIC -- RAW STREAM - View for new customers Delta Table example
-- MAGIC CREATE INCREMENTAL LIVE VIEW customer_v
-- MAGIC COMMENT "View built against raw, streaming Customer data source."
-- MAGIC AS SELECT * FROM STREAM(retail.customer)
-- MAGIC ```  
-- MAGIC Below we use the streaming Autoloader CloudFiles utility.

-- COMMAND ----------

-- MAGIC %md ### 1. BRONZE - Land Raw Data and standardize types
-- MAGIC   
-- MAGIC **Common Storage Format:** Delta  
-- MAGIC **Data Types:** Cast & check Nulls

-- COMMAND ----------

-- BRONZE - CloudFiles AutoLoader reads raw streaming files for "new" customer records
CREATE OR REFRESH STREAMING LIVE TABLE customer_bronze
  (
    id int,
    first_name string,
    last_name string,
    email string,
    channel string,
    active int,
    active_end_date date,
    update_dt timestamp,
    update_user string,
    input_file_name string COMMENT 'Data record was auto-loaded from this input file'
  )
TBLPROPERTIES ("quality" = "bronze")
COMMENT "New customer data incrementally ingested from cloud object storage landing zone"
AS 
SELECT 
    CAST(id AS int),
    first_name,
    last_name,
    email,
    channel,
    CAST(active AS int),
    CAST(active_end_date AS date),
    CAST(update_dt AS timestamp),
    update_user,
    input_file_name() input_file_name
  FROM cloud_files('/Users/glenn.wiebe@databricks.com/ggw_retail/data/in/', 'csv', map('header', 'true', 'schema', 'id int, first_name string, last_name string, email string, channel string, active int, active_end_date date, update_dt timestamp, update_user string, input_file_name string'))
--   FROM cloud_files('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/in/', 'csv', map('header', 'true', 'schema', 'id int, first_name string, last_name string, email string, channel string, active int, active_end_date date, update_dt timestamp, update_user string, input_file_name string'))
;

-- COMMAND ----------

-- MAGIC %md #### External Reference data used when enriching silver

-- COMMAND ----------

-- REFERENCE - View for Sales Channel reference table 
CREATE LIVE VIEW sales_channel_v
COMMENT "View built against Channel reference data."
AS SELECT channelId,
          channelName,
          description
     FROM ggw_retail.channel
;

-- COMMAND ----------

-- MAGIC %md ### 2. SILVER - Cleansed Table
-- MAGIC   
-- MAGIC **Common Storage Format:** Delta  
-- MAGIC **Data Types:** Cast & check Nulls

-- COMMAND ----------

-- SILVER - View against Bronze that will be used to load silver incrementally with APPLY CHANGES INTO
CREATE STREAMING LIVE VIEW customer_bronze2silver_v (
  CONSTRAINT valid_id           EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_active       EXPECT (active BETWEEN 0 AND 1) ON VIOLATION DROP ROW,
  CONSTRAINT valid_channel      EXPECT (sales_channel IS NOT NULL),
  CONSTRAINT valid_first_name   EXPECT (first_name IS NOT NULL),
  CONSTRAINT valid_last_name    EXPECT (last_name IS NOT NULL)
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "View of cleansed Bronze Customer for loading into Silver."
AS SELECT c.id,
          UPPER(c.first_name) as first_name,
          UPPER(c.last_name) as last_name,
          c.email,
          sc.channelName sales_channel,
          c.active,
          c.active_end_date,
          c.update_dt,
          c.update_user,
          current_timestamp() dlt_ingest_dt,
          "CustomerApplyChanges" dlt_ingest_procedure,
          current_user() dlt_ingest_principal
     FROM STREAM(live.customer_bronze) c
     LEFT JOIN live.sales_channel_v sc
       ON c.channel = sc.channelId
;

-- COMMAND ----------

-- SILVER - Incremental Customer table with APPLY CHANGES INTO change handling
CREATE INCREMENTAL LIVE TABLE customer_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customers"
;

APPLY CHANGES INTO live.customer_silver
FROM stream(live.customer_bronze2silver_v)
  KEYS (id)
  APPLY AS DELETE WHEN active = 0
  SEQUENCE BY update_dt
;

-- COMMAND ----------

-- MAGIC %md ### 3. GOLD - Analytics Table
-- MAGIC   

-- COMMAND ----------

-- -- SERVE - Aggregate Customers by Sales Channel 
-- CREATE LIVE TABLE channel_customers_gold
-- COMMENT "Aggregate Customers by Sales Channel."
-- AS SELECT sales_channel,
--           COUNT(1) customer_count,
--           MAX(update_dt) most_recent_customer_update_dt
--      FROM live.customer_silver
--     GROUP BY sales_channel
-- ;
