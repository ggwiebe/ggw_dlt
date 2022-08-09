-- Databricks notebook source
-- MAGIC %md # Delta Live Tables - Department Employees  
-- MAGIC   
-- MAGIC This is the declared definition of the medallion architecture for Employees in a Department.  
-- MAGIC There is a raw streaming source for employees and a reference/master data for Departments.  
-- MAGIC   
-- MAGIC In this DLT Pipeline definition, we:
-- MAGIC - add quality from raw to bronze (check for nulls, etc.)
-- MAGIC - do referential checks and lookup reference information into silver
-- MAGIC - aggregate to create gold reporting tables
-- MAGIC 
-- MAGIC <img src="files/ggwiebe/department/DLT-EmployeeDepartment-graph-small.jpg" alt="Employee Department Delta Live Tables"/>

-- COMMAND ----------

-- MAGIC %md ### x. SOURCE & REFERENCE - Stream & Lookup Data  
-- MAGIC   
-- MAGIC **Source**: Stream of New Employee Records  
-- MAGIC **Enrich**: Reference table of Department Codes->Names 

-- COMMAND ----------

-- REFERENCE - View for department table 
CREATE LIVE VIEW department_v
COMMENT "View built against Deparment reference data."
AS SELECT * FROM ggw_department.department

-- COMMAND ----------

-- RAW STREAM - View for new customers
CREATE INCREMENTAL LIVE VIEW employee_raw_stream_v
COMMENT "View built against raw, streaming Employee data source."
AS SELECT * FROM STREAM(ggw_department.employee_raw)

-- COMMAND ----------

-- MAGIC %md ### 1. BRONZE - Land Stream  
-- MAGIC   
-- MAGIC **Common Storage Format:** Delta  
-- MAGIC **Data Types:** Cast & check Nulls

-- COMMAND ----------

-- BRONZE - Table based on stream of new customers
CREATE INCREMENTAL LIVE TABLE employee_bronze (
  CONSTRAINT valid_dept_ID      EXPECT (deptId IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_first_name   EXPECT (fname IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_last_name    EXPECT (lname IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Bronze table from streaming Employee view."
AS SELECT UPPER(fName) as fName,
          UPPER(lName) as lName,
          deptId
     FROM STREAM(live.employee_raw_stream_v)

-- COMMAND ----------

-- MAGIC %md ### 2. SILVER - Clean with Business Rules  
-- MAGIC   
-- MAGIC **Business Rule - 1:** Create Full Name  
-- MAGIC **Business Rule - 2:** Translate Department ID to Department Name  

-- COMMAND ----------

-- SILVER - Table based on JOIN between bronze customers and department for lookup
CREATE INCREMENTAL LIVE TABLE employee (
  CONSTRAINT valid_dept_name EXPECT (deptName IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Cleansed Silver table from Employee_Bronze and Department."
AS SELECT CONCAT(e.fName, ' ', e.lName) AS fullName,
          d.deptName
           , current_timestamp() AS updateDT 
     FROM STREAM(live.employee_bronze) e
LEFT JOIN live.department_v d
       ON d.deptId = e.deptId

-- COMMAND ----------

-- MAGIC %md ### 3. GOLD - Create Aggregate Table [Count of Employees by Department]  
-- MAGIC   
-- MAGIC **Business Rule - 1:** Aggregate count of employees by department

-- COMMAND ----------

-- GOLD - Aggregate table to keep track of employees per department
CREATE LIVE TABLE employee_countby_dept
COMMENT "Aggregate Gold table from Employee showing employee count by Department."
AS SELECT deptName,
          count(*) AS empCount
     FROM live.employee
 GROUP BY deptName

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Department Employees - Next Iteration!
-- MAGIC    
-- MAGIC We have tried out our pipeline, and we want to make some changes:  
-- MAGIC 
-- MAGIC 1. Data Quality Checks - don't drop the data
-- MAGIC 2. Another aggregate - get an employee count aggregate for our dashboard app

-- COMMAND ----------

-- BRONZE #2 - Table based on stream of new customers - NO VIOLATION DROP
CREATE INCREMENTAL LIVE TABLE employee_bronze_nodrop (
  CONSTRAINT valid_dept_ID      EXPECT (deptId IS NOT NULL),
  CONSTRAINT valid_first_name   EXPECT (fname IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_last_name    EXPECT (lname IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Bronze table (without dropping failed messages) from streaming Employee view."
AS SELECT UPPER(fName) as fName,
          UPPER(lName) as lName,
          deptId
     FROM STREAM(live.employee_raw_stream_v)

-- COMMAND ----------

-- -- GOLD #2 - Aggregate table to keep track of employee count
-- CREATE LIVE TABLE employee_count
-- COMMENT "Aggregate Gold table from Employee showing total employee count."
-- AS SELECT count(*) AS empCount
--      FROM live.employee

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC ### Change Stream Source

-- COMMAND ----------

-- -- Create RAW STREAM View for new customers
-- CREATE INCREMENTAL LIVE VIEW employee_raw_stream_v
-- COMMENT "View built against raw, streaming Employee data source."
-- AS SELECT * FROM cloud_files('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_department/raw/employee/', 'csv', map("header", "true"))
