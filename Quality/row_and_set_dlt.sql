-- Databricks notebook source
-- MAGIC %md # DLT Notebook - Row & Set Based DLT Quality Checking

-- COMMAND ----------

-- Not UC compatible, so neither of these (nor cat.db.table syntax) work:
-- spark.databricks.sql.initial.catalog.name hive_metastore
-- USE CATALOG ggw;

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE employee_bronze (
--   CONSTRAINT valid_hire_date EXPECT (id IS NULL)
)
COMMENT "The customers buying finished products ingested in incremental with Auto Loader"
TBLPROPERTIES ("quality" = "bronze")
AS 
  SELECT *,
         now() AS ingest_ts,
         'Workspace/Repos/glenn.wiebe@databricks.com/db-fe-dlt/dlt/Quality/row_and_set_dlt' AS ingest_src
    FROM STREAM(ggw_department.employee_source);

-- COMMAND ----------

CREATE LIVE VIEW employee_deptmgrcount_bronze (
--   CONSTRAINT no_multiple_managers EXPECT (count <= 1)
)
COMMENT "The customers buying finished products ingested in incremental with Auto Loader"
TBLPROPERTIES ("quality" = "bronze-aggregate")
AS 
  SELECT department,
         count(*) AS ManagerCount
    FROM ggw_department.employee_source
   WHERE position = 'Manager'
   GROUP BY department
;

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE employee_silver (
  CONSTRAINT valid_hire_date EXPECT (hire_date < update_dt),
  CONSTRAINT no_multiple_managers EXPECT (ManagerCount <= 1)
)
COMMENT "Employee cleansed table with both Row-level and Set/Aggregate-level quality rules applied."
TBLPROPERTIES ("quality" = "silver")
AS 
  SELECT e.*,
         d.ManagerCount
    FROM STREAM(LIVE.employee_bronze) e
    LEFT OUTER JOIN LIVE.employee_deptmgrcount_bronze d
         ON e.department = d.department
