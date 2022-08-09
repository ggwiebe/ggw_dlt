-- Databricks notebook source
-- MAGIC %md ### 2. b) SILVER - Quarantine Table
-- MAGIC   
-- MAGIC Same constraints as Silver table (reverse the predicate)

-- COMMAND ----------

-- SILVER - View against Bronze that will be used to load silver incrementally with APPLY CHANGES INTO
--          Take the opposite of all regular expectations then OR all of them; 
--          So if any bad things keep, else DROP ROW (from quarantine)
CREATE INCREMENTAL LIVE TABLE customer_quarantine (
  CONSTRAINT invalid_anything   EXPECT (Continent IS NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver_quarantine")
COMMENT "Country Case Counts with data violations (quarantine source)."
AS SELECT cc.*,
          c.Continent,
          current_timestamp() dlt_ingest_dt,
          "Ingest2Egress-Live" dlt_ingest_procedure,
          current_user() dlt_ingest_principal
     FROM STREAM(live.country_daily_casecount_bronze) cc
     LEFT JOIN live.country_reference c
            ON cc.Country = c.Country
