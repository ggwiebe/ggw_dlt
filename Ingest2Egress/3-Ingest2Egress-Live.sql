-- Databricks notebook source
-- MAGIC %md # Transform Data in Pipeline
-- MAGIC   
-- MAGIC ![DLT Process Flow](https://raw.githubusercontent.com/ggwiebe/db-fe-dlt/main/dlt/applychanges/images/DLT_Process_Flow.png)

-- COMMAND ----------

-- MAGIC %md ## Reference
-- MAGIC   
-- MAGIC Read from Raw and Write to Reference

-- COMMAND ----------

-- REFERENCE - Data for Country reference table 
CREATE INCREMENTAL LIVE TABLE country_reference
TBLPROPERTIES ("quality" = "reference")
COMMENT "Country Reference Table"
AS 
SELECT *,
       current_timestamp() input_dt,
       input_file_name() input_file_name
  FROM cloud_files('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_ods/data/in/Countries-Continents*', 'csv', 
       map('header', 'true', 'schema', 'Continent string, Country string'))

-- COMMAND ----------

-- MAGIC %md ## Bronze
-- MAGIC   
-- MAGIC Read from Raw and Write to Bronze

-- COMMAND ----------

-- BRONZE - CloudFiles AutoLoader reads raw streaming files for "new" customer records
CREATE INCREMENTAL LIVE TABLE country_daily_casecount_bronze
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Latest Case Counts by Country ingested from cloud object storage landing zone"
AS 
SELECT *,
       current_timestamp() input_dt,
       input_file_name() input_file_name
  FROM cloud_files('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_ods/data/in/countries-aggregated*', 'csv', 
       map('header', 'true', 'schema', 'Date date, Country string, Confirmed int, Recovered int, Deaths int'))

-- COMMAND ----------

-- MAGIC %md ## Silver  
-- MAGIC   
-- MAGIC After bronze daily data and reference data, create silver table

-- COMMAND ----------

-- SILVER - View against Bronze that will be used to load silver incrementally with APPLY CHANGES INTO
CREATE INCREMENTAL LIVE TABLE country_cases_silver (
  CONSTRAINT valid_continent    EXPECT (Continent IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Transformed into Silver & Quality constrained"
AS SELECT cc.*,
          c.Continent,
          current_timestamp() dlt_ingest_dt,
          "Ingest2Egress-Live" dlt_ingest_procedure,
          current_user() dlt_ingest_principal
     FROM STREAM(live.country_daily_casecount_bronze) cc
     LEFT JOIN live.country_reference c
            ON cc.Country = c.Country

-- COMMAND ----------

-- MAGIC %md ## Gold  
-- MAGIC   
-- MAGIC Aggregate Output Table

-- COMMAND ----------

-- GOLD - Query that aggregates our Silver data and generates necessary publish dataset
CREATE LIVE TABLE continent_deaths_gold (
)
TBLPROPERTIES ("quality" = "gold")
COMMENT "Aggregate deaths by continent - taking max death count by continent and then listing the offending country"
AS 
-- Get the Country that has the largest deaths per continent
SELECT current_date() AsOfDate,
       cd.Continent,
       cd.ContinentMaxDeaths,
       cc.Country
  FROM LIVE.country_cases_silver cc,
       ( SELECT Continent,
                MAX(Deaths) ContinentMaxDeaths
           FROM LIVE.country_cases_silver
          GROUP BY Continent ) cd 
 WHERE cc.Continent = cd.Continent
   AND cc.Deaths = cd.ContinentMaxDeaths
;
