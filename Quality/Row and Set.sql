-- Databricks notebook source
-- MAGIC %md # Distinquish between Row-based and Set-based Quality Rules

-- COMMAND ----------

-- MAGIC %md ## Create & Use catalog and database

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS GGW;
USE CATALOG ggw;

CREATE DATABASE IF NOT EXISTS ggw_quality;
USE DATABASE ggw_quality;

-- Check it
SELECT current_catalog(), current_database()

-- COMMAND ----------

-- MAGIC %md ## Create Source Table of Events

-- COMMAND ----------

-- Quality Rules
-- Row:
--   * Hire Date cannot be greater than current date
-- Set:
--   * Only one manager per department
CREATE TABLE employee_source (
  userId     INTEGER   NOT NULL,
  name       STRING    NOT NULL,
  department STRING            ,
  position   STRING            ,
  hire_date  DATE              ,
  operation  STRING    NOT NULL,
  update_dt  TIMESTAMP NOT NULL,
  update_by  STRING    NOT NULL
)


-- COMMAND ----------

INSERT INTO employee_source
VALUES
  -- Initial load
  (123, "Isabel",   "Sales",         "Manager", '2022-01-01', "INSERT", now(), 'Workspace/Repos/glenn.wiebe@databricks.com/db-fe-dlt/dlt/Utilities/Row and Set'),
  (124, "Raul",     "FieldEng",      "Manager", '2022-02-01', "INSERT", now(), 'Workspace/Repos/glenn.wiebe@databricks.com/db-fe-dlt/dlt/Utilities/Row and Set')

-- COMMAND ----------

INSERT INTO employee
VALUES
  -- One new user in Sales
  (125, "Mercedes", "Sales",         "Member",  '2022-02-02', "INSERT", now())
;

-- COMMAND ----------

INSERT INTO employee
VALUES
  -- Sales Manager removed from the system, and Mercedes promoted Manager (no other details, like dept or hire-date change)
  (123, null,       null,            null,                   "DELETE", now()),
  (125, "Mercedes", null,            "Manager", null,        "UPDATE", now())
;

-- COMMAND ----------

-- MAGIC %md ## Insert Bad Data. 
-- MAGIC   
-- MAGIC a) Bad row - fails the valid-hire-date rule  
-- MAGIC b) Bad set - Set[By Department] only one manager at a time  

-- COMMAND ----------

INSERT INTO employee
VALUES
  -- This batch of updates came out of order - the above batch at sequenceNum 5 will be the final state.
  (126, "Levy",     "FieldEng",     "Manager", '2022-02-02', "INSERT", now())


-- COMMAND ----------

-- MAGIC %md ## Run Some Queries

-- COMMAND ----------

CREATE TABLE employee (
  userId     INTEGER   NOT NULL,
  name       STRING    NOT NULL,
  department STRING            ,
  function   STRING            ,
  operation  STRING    NOT NULL,
  update_dt  TIMESTAMP NOT NULL,
  update_by  STRING    NOT NULL
)

