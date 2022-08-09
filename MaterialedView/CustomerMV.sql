-- Databricks notebook source
-- MAGIC %md ## Define View

-- COMMAND ----------

-- SILVER - Table based on stream of new customers
CREATE INCREMENTAL LIVE VIEW customer_gold_mv
TBLPROPERTIES ("quality" = "gold")
COMMENT "Materialized View based on customer"
AS SELECT id,
          first_name || ' ' || last_name as full_name,
          email,
          active,
          update_dt update_dt
     FROM STREAM(ggw_retail.customer)

-- COMMAND ----------

-- MAGIC %md ## Materialize all changes from source to materialized view
-- MAGIC   

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE customer_gold
TBLPROPERTIES ("quality" = "gold")
COMMENT "Transformed for querying"
;

-- COMMAND ----------

APPLY CHANGES INTO live.customer_gold
FROM stream(live.customer_gold_mv)
  KEYS (id)
  APPLY AS DELETE WHEN active = 0
  SEQUENCE BY update_dt
;
