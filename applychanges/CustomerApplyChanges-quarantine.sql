-- Databricks notebook source
-- MAGIC %md ### 2. b) SILVER - Quarantine Table
-- MAGIC   
-- MAGIC Same constraints as Silver table (reverse the predicate)

-- COMMAND ----------

-- SILVER - View against Bronze that will be used to load silver incrementally with APPLY CHANGES INTO
--          Take the opposite of all regular expectations then OR all of them; 
--          So if any bad things keep, else DROP ROW (from quarantine)
CREATE INCREMENTAL LIVE TABLE customer_quarantine (
  CONSTRAINT invalid_anything   EXPECT (  
                                   (id IS NULL)
                                   OR (active NOT IN (0,1))
                                   OR (channel IS NULL OR sc.channelName IS NULL)
                                   OR (first_name IS NULL)
                                   OR (last_name IS NULL) 
                                ) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver_quarantine")
COMMENT "View of cleansed Bronze Customer violations (quarantine source)."
AS SELECT c.id,
          c.first_name,
          c.last_name,
          c.email,
          c.channel,
          sc.channelId, 
          sc.channelName,
          c.active,
          c.active_end_date,
          c.update_dt,
          c.update_user,
          current_timestamp() dlt_ingest_dt,
          "CustomerApplyChanges-quarantine" dlt_ingest_procedure,
          current_user() dlt_ingest_principal
     FROM STREAM(live.customer_bronze) c
     LEFT JOIN live.sales_channel_v sc
       ON c.channel = sc.channelId
