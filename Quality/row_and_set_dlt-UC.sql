-- Databricks notebook source
-- MAGIC %md # DLT Notebook - Row & Set Based DLT Quality Checking

-- COMMAND ----------

-- Not UC compatible, so neither of these (nor cat.db.table syntax) work:
-- spark.databricks.sql.initial.catalog.name hive_metastore

-- I am leaving this in, so we make sure to not use it until UC - DLT integration goes through!
USE CATALOG ggw;

-- COMMAND ----------

CREATE INCREMENTAL LIVE VIEW employee_row_bronze (
--   CONSTRAINT valid_hire_date EXPECT (id IS NULL)
)
COMMENT "The customers buying finished products ingested in incremental with Auto Loader"
TBLPROPERTIES ("quality" = "bronze")
AS 
  SELECT *,
         now() AS ingest_ts,
         'Workspace/Repos/glenn.wiebe@databricks.com/db-fe-dlt/dlt/Quality/row_and_set_dlt' AS ingest_src
    FROM STREAM(ggw_quality.employee_source);

-- COMMAND ----------

CREATE INCREMENTAL LIVE VIEW employee_dept_set_bronze (
--   CONSTRAINT no_multiple_managers EXPECT (count <= 1)
)
COMMENT "The customers buying finished products ingested in incremental with Auto Loader"
TBLPROPERTIES ("quality" = "bronze")
AS 
  SELECT department,
         count(*) AS ManagerCount
    FROM ggw_quality.employee_source
   GROUP BY department;

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE employee_dept_mgrcnt_bronze (
  CONSTRAINT valid_hire_date EXPECT (id IS NULL),
  CONSTRAINT no_multiple_managers EXPECT (count <= 1)
)
COMMENT "The customers buying finished products ingested in incremental with Auto Loader"
TBLPROPERTIES ("quality" = "bronze")
AS 
  SELECT *
    FROM LIVE.employee_row_bronze e,
    INNER JOIN LIVE.employee_dept_set_bronze d
      ON e.department = d.department
