-- Databricks notebook source
-- MAGIC %md ## x. Setup for Database  

-- COMMAND ----------

CREATE WIDGET TEXT root_location DEFAULT "/Users/glenn.wiebe@databricks.com/";
CREATE WIDGET TEXT db_name DEFAULT "ggw_retail";
CREATE WIDGET TEXT data_loc DEFAULT "/data";
-- REMOVE WIDGET old

-- COMMAND ----------

-- DROP DATABASE $db_name CASCADE;
CREATE DATABASE $db_name
LOCATION "$root_location/$db_name/$db_name.db";

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED $db_name

-- COMMAND ----------

-- MAGIC %md ## Delta Lake based source table  
-- MAGIC   
-- MAGIC This is a data model for the above csv input files.

-- COMMAND ----------

-- 0. Create customer table
-- DROP TABLE $db_name.customer;
CREATE TABLE $db_name.customer (
  id INTEGER NOT NULL,
  first_name varchar(255) NOT NULL,
  last_name varchar(255) NOT NULL,
  email varchar(255) NOT NULL,
  active INTEGER,
  update_dt timestamp GENERATED ALWAYS AS (now()),
  update_user varchar(128) GENERATED ALWAYS AS (current_user())
);

-- COMMAND ----------

SELECT *
  FROM $db_name.customer
;

-- COMMAND ----------

-- MAGIC %md ## CREATE/START DLT PIPELINE!!!  
-- MAGIC   
-- MAGIC Once the above infrastructure is in place, start the pipeline (in continuous mode, or start after each of the next steps)

-- COMMAND ----------

-- MAGIC %md ## 1. Insert

-- COMMAND ----------

-- 1. insert first records
INSERT INTO $db_name.customer
(id, first_name, last_name, email, active)
VALUES
    (1001, 'Glenn', 'Wiebe', 'ggwiebe@gmail.com', 1),
    (1002, 'Graeme', 'Wiebe', 'glenn@wiebes.net', 1)
;

-- COMMAND ----------

-- 1.+ Select these first records & the dlt moved records
SELECT *
  FROM $db_name.customer
 ORDER BY update_dt, id
;

-- COMMAND ----------

-- Query Materialized view of this
SELECT *
  FROM $db_name.customer_gold
 ORDER BY update_dt, id
;

-- COMMAND ----------

-- MAGIC %md ## 2. Append

-- COMMAND ----------

-- 2. insert more records
INSERT INTO $db_name.customer
(id, first_name, last_name, email, active)
VALUES
    (1003, 'Dillon', 'Bostwick', 'dillon@databricks.com', 1),
    (1004, 'Franco', 'Patano', 'franco.patano@databricks.com', 1)
 ;

-- COMMAND ----------

-- Query Materialized view of this
SELECT *
  FROM $db_name.customer_gold
 ORDER BY update_dt, id
;

-- COMMAND ----------

-- MAGIC %md ## 3. Update

-- COMMAND ----------

-- 3. insert the update records
INSERT INTO $db_name.customer
(id, first_name, last_name, email)
VALUES
    (1002, 'Glenn', 'Wiebe', 'glenn@wiebes.net')
;

-- -- 3. update record
-- UPDATE $db_name.customer
--    SET first_name = 'Glenn'
--  WHERE id = 1002

-- COMMAND ----------

-- 1.+ Select these first records & the dlt moved records
SELECT *
  FROM $db_name.customer
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- MAGIC %md ## 4. Delete

-- COMMAND ----------

-- 4. insert the Delete record
INSERT INTO $db_name.customer
(id, first_name, last_name, email, active)
VALUES
    (1002, 'Glenn', 'Wiebe', 'glenn@wiebes.net', 0)
;

-- -- 4.+ Delete the second Glenn record
-- UPDATE $db_name.customer
--  WHERE id = 1002
-- ;
