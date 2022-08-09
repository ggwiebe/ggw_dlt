-- Databricks notebook source
-- MAGIC %md ## Delta Live Tables - Employee & Department Demo  
-- MAGIC   
-- MAGIC <img src="files/ggwiebe/department/DLT-EmployeeDepartment-graph-small.jpg" alt="Employee Department Delta Live Tables"/>
-- MAGIC 
-- MAGIC In this demo, we start with:
-- MAGIC - A vendor-supplied source of streaming raw new employee records (e.g. a set of Cloud_Files csv files, a Kafka topic, a streamable table, etc.)
-- MAGIC - An Master Data Management Department table
-- MAGIC   
-- MAGIC The DLT solution will take these existing sources and create the associated target infrastructure from the defined DLT configuration.  
-- MAGIC   
-- MAGIC ### DEMO FLOW
-- MAGIC x. Assume the database is created and context changed to that location  
-- MAGIC y. To clean up delete the DLT created tables, and drop the employee_raw stream source AND delete the directory for this unmanaged table
-- MAGIC 1. Start with just these two tables: Department & Employee_raw
-- MAGIC 2. Create Data Pipeline Configuration (in the editor of your choice)
-- MAGIC 3. Deploy Pipeline (using Databricks CLI)
-- MAGIC 4. Monitor Pipeline from Workspace (Jobs/Pipelines)
-- MAGIC 5. Monitor SQL tables (should now be created); re-query contents of employee table
-- MAGIC 6. Inject new Good data
-- MAGIC 7. Inject new Problem data
-- MAGIC 8. Inject bad data

-- COMMAND ----------

-- DROP TABLE department;
-- DROP TABLE employee_raw;
-- DROP DATABASE ggw_department
-- CREATE DATABASE ggw_department 
--   LOCATION 'dbfs:/Users/glenn.wiebe@databricks.com/ggw_department/ggw_department.db'
--   LOCATION 'abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_department/ggw_department.db';

USE ggw_department;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # To reset the DLT infrastructure, delete these!
-- MAGIC dbutils.fs.rm("/Users/glenn.wiebe@databricks.com/dlt_department/autoloader", True)
-- MAGIC dbutils.fs.rm("/Users/glenn.wiebe@databricks.com/dlt_department/tables", True)
-- MAGIC dbutils.fs.rm("/Users/glenn.wiebe@databricks.com/dlt_department/checkpoints", True)
-- MAGIC dbutils.fs.rm("/Users/glenn.wiebe@databricks.com/dlt_department/system", True)
-- MAGIC 
-- MAGIC dbutils.fs.ls("/Users/glenn.wiebe@databricks.com/dlt_department/")

-- COMMAND ----------

-- Drop these DLT tables to reset the demo
DROP TABLE IF EXISTS ggw_department.Employee_bronze;
DROP TABLE IF EXISTS ggw_department.Employee_bronze_nodrop;
DROP TABLE IF EXISTS ggw_department.Employee;
DROP TABLE IF EXISTS ggw_department.Employee_CountBy_Dept;
DROP TABLE IF EXISTS ggw_department.Employee_Count;

-- COMMAND ----------

DROP TABLE IF EXISTS ggw_department.department;

CREATE TABLE IF NOT EXISTS ggw_department.department (
  deptId integer, 
  deptName string
);

INSERT INTO ggw_department.department VALUES (1, 'ACCOUNTING');
INSERT INTO ggw_department.department VALUES (2, 'R&D');
INSERT INTO ggw_department.department VALUES (3, 'SALES');

-- COMMAND ----------

SELECT * FROM ggw_department.department;
-- DESCRIBE DETAIL ggw_department.department;

-- COMMAND ----------

DROP TABLE IF EXISTS ggw_department.employee_raw;

CREATE TABLE IF NOT EXISTS ggw_department.employee_raw (
  fname string, 
  lname string,
  deptId integer
);

INSERT INTO ggw_department.employee_raw VALUES ('Jane', 'Doe', 2);
INSERT INTO ggw_department.employee_raw VALUES ('John', 'Smith', 1);
INSERT INTO ggw_department.employee_raw VALUES ('Ranjit', 'Khan', 3);

-- COMMAND ----------

SELECT * FROM ggw_department.Employee_raw;
-- DESCRIBE DETAIL ggw_department.Employee_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Wait until Pipeline is Operational  
-- MAGIC   
-- MAGIC ### Verify existing data has been propagated by pipeline

-- COMMAND ----------

SELECT e.fullName,
       e.deptName,
       c.count
  FROM employee e, 
       employee_countby_dept c
 WHERE c.deptName = e.deptName
;

-- COMMAND ----------

-- MAGIC %md ### Add new raw data [CLEAN]  
-- MAGIC   
-- MAGIC After the insert to raw...  
-- MAGIC Verify that the new has been propagated by pipeline

-- COMMAND ----------

INSERT INTO ggw_department.Employee_raw VALUES ('Glenn', 'Wiebe', 3);
INSERT INTO ggw_department.Employee_raw VALUES ('Glenn', 'Koehler', 1);
INSERT INTO ggw_department.Employee_raw VALUES ('Mladen', 'Kovacevic', 3);

-- COMMAND ----------

SELECT e.fullName,
       e.deptName,
       c.count
  FROM Employee e, 
       Employee_countby_dept c
 WHERE c.deptName = e.deptName
;

-- COMMAND ----------

-- MAGIC %md ### Add new raw data [BAD-Department]  
-- MAGIC   
-- MAGIC Insert new bad department - *** This record gets dropped ***

-- COMMAND ----------

INSERT INTO ggw_department.Employee_raw VALUES ('John', 'Doe', null);
INSERT INTO ggw_department.Employee_raw VALUES ('Teresa', 'Mills', 3);
INSERT INTO ggw_department.Employee_raw VALUES ('Bill', 'Smith', 1);

-- COMMAND ----------

SELECT e.fullName,
       e.deptName,
       c.count
  FROM Employee e, 
       Employee_countby_dept c
 WHERE c.deptName = e.deptName
;
-- You will see that there are only 8 rows, and the 9th row, John Doe is NOT propagated

-- COMMAND ----------

-- MAGIC %md ### Add new raw data [bad-fName]  
-- MAGIC   
-- MAGIC Insert new bad employee [Missing First Name] - *** THIS FAILS THE LOAD ***  
-- MAGIC   
-- MAGIC In the Pipelines Console, you will see the failure and the stream is stopped

-- COMMAND ----------

INSERT INTO ggw_department.Employee_raw VALUES (null, 'Wiebe', null);

-- COMMAND ----------

SELECT e.fullName,
       e.deptName,
       c.count
  FROM Employee e, Employee_countby_dept c
 WHERE c.deptName = e.deptName
;
-- You will see that there are only 8 rows, and the 9th row, John Doe is NOT propagated

-- COMMAND ----------

-- MAGIC %md ### Make this pipeline better!!!  
-- MAGIC   
-- MAGIC Go to DLT Definition and:
-- MAGIC - Add another quality checking table (no drop on bad data)
-- MAGIC - Add another gold aggregate
-- MAGIC   
-- MAGIC Re-start pipeline and see new pipeline image.  
-- MAGIC Add some new bad data and see it show up in no-drop table. 

-- COMMAND ----------

INSERT INTO ggw_department.Employee_raw VALUES ('Saul', 'Goodman', 2);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #display(spark.readStream.format('delta').load('/Users/glenn.wiebe@databricks.com/dlt_department/tables/employee_bronze'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #display(spark.readStream.format('delta').load('/Users/glenn.wiebe@databricks.com/dlt_department/tables/employee_silver/'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #display(spark.readStream.format('delta').load('/Users/glenn.wiebe@databricks.com/ggwDepartment/tables/department_summary/'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('/Users/glenn.wiebe@databricks.com/dlt_department/')

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_department/raw'

-- COMMAND ----------

select *
  from csv.'abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_department/raw/employee-new-good.csv'

-- COMMAND ----------

CREATE TEMPORARY VIEW employee_new
USING CSV
OPTIONS (path "abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_department/raw/employee-new-good.csv", header "true", mode "FAILFAST")

-- COMMAND ----------

SELECT *
  FROM employee_new;
