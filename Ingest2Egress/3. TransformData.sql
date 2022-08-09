-- Databricks notebook source
-- MAGIC %md # Transform Data in Pipeline
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md ## Config
-- MAGIC   
-- MAGIC Setup environment variables

-- COMMAND ----------

CREATE WIDGET TEXT db_name   DEFAULT = "ggw_ods";
CREATE WIDGET TEXT date_from DEFAULT = "2021-01-01";
CREATE WIDGET TEXT date_to   DEFAULT = "2021-12-31";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Get parameter from Job parm passed in
-- MAGIC db_name = dbutils.widgets.get("db_name")
-- MAGIC print("Using Database: {}".format(db_name))
-- MAGIC date_from = dbutils.widgets.get("date_from")
-- MAGIC date_to = dbutils.widgets.get("date_to")
-- MAGIC print("Using date_from: {} to date_to: {}".format(date_from,date_to))

-- COMMAND ----------

SELECT *
  FROM $db_name.country_daily_casecount
 WHERE Date = getArgument("date_to")

-- COMMAND ----------

-- MAGIC %md ## Query source table  
-- MAGIC   
-- MAGIC After Ingest2Table, we should see a table in the list 

-- COMMAND ----------

SELECT *
  FROM $db_name.country
;

-- COMMAND ----------

-- Get the Country that has the largest deaths per continent
SELECT current_date() AsOfDate,
       c.Continent,
       cdcc.Country CountryWithMax,
       cd.ContinentMaxDeaths
  FROM $db_name.country_daily_casecount cdcc,
       $db_name.country c,
       ( SELECT c.Continent,
                MAX(Deaths) ContinentMaxDeaths
           FROM $db_name.country_daily_casecount ccc,
                $db_name.country c
          WHERE ccc.Country = c.Country
            AND `Date` BETWEEN '$date_from' AND '$date_to' 
          GROUP BY c.Continent
       ) cd
 WHERE cdcc.Country = c.Country
   AND c.Continent = cd.Continent
   AND cdcc.Deaths = cd.ContinentMaxDeaths

-- COMMAND ----------

-- MAGIC %md ## Generate Aggregate Output Table

-- COMMAND ----------

CREATE OR REPLACE TABLE $db_name.ContinentMaxDeaths
AS (
    -- Get the Country that has the largest deaths per continent
    SELECT current_date() AsOfDate,
           c.Continent,
           cdcc.Country CountryWithMax,
           cd.ContinentMaxDeaths
      FROM $db_name.country_daily_casecount cdcc,
           $db_name.country c,
           ( SELECT c.Continent,
                    MAX(Deaths) ContinentMaxDeaths
               FROM $db_name.country_daily_casecount ccc,
                    $db_name.country c
              WHERE ccc.Country = c.Country
                AND `Date` BETWEEN '$date_from' AND '$date_to' 
              GROUP BY c.Continent
           ) cd
     WHERE cdcc.Country = c.Country
       AND c.Continent = cd.Continent
       AND cdcc.Deaths = cd.ContinentMaxDeaths
);
