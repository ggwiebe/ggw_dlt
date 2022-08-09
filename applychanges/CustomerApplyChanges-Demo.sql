-- Databricks notebook source
-- MAGIC %md ## DLT Architecture  
-- MAGIC   
-- MAGIC ![DLT Technical Architecture](https://raw.githubusercontent.com/ggwiebe/db-fe-dlt/main/dlt/applychanges/images/DLT_Technical_Architecture.png)

-- COMMAND ----------

-- MAGIC %md ## x. Setup for Database  

-- COMMAND ----------

-- CREATE WIDGET TEXT root_location DEFAULT "/Users/glenn.wiebe@databricks.com/";
CREATE WIDGET TEXT root_location DEFAULT "abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/";
CREATE WIDGET TEXT db_name DEFAULT "ggw_retail";
CREATE WIDGET TEXT data_loc DEFAULT "/data";
-- REMOVE WIDGET old

-- COMMAND ----------

-- MAGIC %python
-- MAGIC db_name = dbutils.widgets.get('db_name')
-- MAGIC 
-- MAGIC print("Running CustomerApplyChanges ")

-- COMMAND ----------

-- DROP DATABASE $db_name CASCADE;
CREATE DATABASE IF NOT EXISTS $db_name
LOCATION "$root_location/$db_name/$db_name.db";

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED $db_name;

-- COMMAND ----------

-- MAGIC %md ### Create Customer Channel reference table. 
-- MAGIC   
-- MAGIC Have a table to use to show joins and some analytic dimensionality

-- COMMAND ----------

-- THIS IS NOW A REFERENCE LOAD DLT PIPELINE

-- -- Customer Channel Reference Table
-- -- DROP TABLE $db_name.channel;
-- CREATE TABLE IF NOT EXISTS $db_name.channel (
--   channelId integer, 
--   channelName string,
--   description string
-- );
-- INSERT INTO $db_name.channel VALUES (1, 'RETAIL', 'Customer originated from Retail Stores');
-- INSERT INTO $db_name.channel VALUES (2, 'WEB', 'Customer originated from Online properties');
-- INSERT INTO $db_name.channel VALUES (3, 'PARTNER', 'Customer referred from Partners');
-- INSERT INTO $db_name.channel VALUES (9, 'OTHER', 'Unattributed customer origination');

-- COMMAND ----------

-- MAGIC %md ## y. Setup for CloudFiles  
-- MAGIC   
-- MAGIC e.g. for ggw_retail, use these:
-- MAGIC ```
-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail
-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail/data
-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail/data/in
-- MAGIC ```
-- MAGIC   
-- MAGIC Sample data (insert, append, update & delete) in $db_name/data;  
-- MAGIC Copy the individual files in sequence, to emulate a series of transactions arriving;  
-- MAGIC Copy from $db_name/data to the $db_name/data/in folder for CloudFiles to pickup-up each individual file in order (and keep track of each)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # dbutils.fs.mkdirs('/Users/glenn.wiebe@databricks.com/{}/data/in'.format(db_name))
-- MAGIC # dbutils.fs.mkdirs('/Users/glenn.wiebe@databricks.com/{}/data/out'.format(db_name))
-- MAGIC dbutils.fs.ls('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/{}/data'.format(db_name))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("ggw_retail.customers_source; path: {}".format("$root_location/$db_name/$data_loc/*.csv"))

-- COMMAND ----------

-- Create a "table" definition against all CSV files in the data location; This emulates the source system (pre-load, all data that will be loaded)
DROP TABLE $db_name.customers_source ;
CREATE TABLE $db_name.customers_source 
  (
      id int, first_name string, last_name string, email string, channel string, active int, active_end_date date, update_dt timestamp, update_user string
  )
 USING CSV
OPTIONS (
    path "$root_location/$db_name/$data_loc/*.csv",
    header "true",
    -- inferSchema "true",
    mode "FAILFAST",
    schema 'id int, first_name string, last_name string, email string, channel string, active int, active_end_date date, update_dt timestamp, update_user string, '
  )
;


-- COMMAND ----------

SELECT *
  FROM $db_name.customers_source
 ORDER BY update_dt, id ASC;

-- COMMAND ----------

-- MAGIC %md ## CREATE/START DLT PIPELINE!!!  
-- MAGIC   
-- MAGIC Once the above infrastructure is in place, start the pipeline (in continuous mode, or start after each of the next steps)

-- COMMAND ----------

-- MAGIC %md ## 1. Copy in first set of records - Insert

-- COMMAND ----------

-- MAGIC %fs cp abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/customer-1-insert.csv abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/in/

-- COMMAND ----------

-- Create a "table" definition against all CSV files in the cloudFiles location (e.g. /data/in)
-- This cannot be done until some records are in /data/in
DROP TABLE $db_name.customers_raw;
CREATE TABLE $db_name.customers_raw 
  (
      id int, first_name string, last_name string, email string, channel string, active int, active_end_date date, update_dt timestamp, update_user string
  )
 USING CSV
OPTIONS (
    path "$root_location/$db_name/$data_loc/in/*.csv",
    header "true",
    -- inferSchema "true",
    mode "PERMISSIVE", -- "FAILFAST",
    schema 'id int, first_name string, last_name string, email string, channel string, active int, active_end_date date, update_dt timestamp, update_user string, input_file_name string'
  )
;


-- COMMAND ----------

REFRESH TABLE $db_name.customers_raw;
SELECT * 
  FROM $db_name.customers_raw
 ORDER BY update_dt, id ASC
;  

-- COMMAND ----------

-- MAGIC %md ### Start DLT if using triggered approach

-- COMMAND ----------

SELECT * 
  FROM $db_name.customer_bronze
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

SELECT * 
  FROM $db_name.customer_silver
 ORDER BY update_dt, id ASC
;  

-- COMMAND ----------

-- MAGIC %md ## 2. Copy in second set of records - Append

-- COMMAND ----------

-- MAGIC %fs cp abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/customer-2-append.csv abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/in/

-- COMMAND ----------

-- MAGIC %md ### Again start pipeline for latest records

-- COMMAND ----------

-- Check Raw
REFRESH TABLE $db_name.customers_raw;
SELECT * 
  FROM ggw_retail.customers_raw
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Bronze 
SELECT * 
  FROM ggw_retail.customer_bronze
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Silver 
SELECT * 
  FROM ggw_retail.customer_silver
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check GOLD 
SELECT * 
  FROM ggw_retail.channel_customers_gold
;

-- COMMAND ----------

-- MAGIC %md ## 3. Copy in third set of records - Update

-- COMMAND ----------

-- MAGIC %fs cp abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/customer-3-update.csv abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/in/

-- COMMAND ----------

-- MAGIC %md ### Yet again start pipeline

-- COMMAND ----------

-- Check Raw
REFRESH TABLE $db_name.customers_raw;
SELECT * 
  FROM ggw_retail.customers_raw
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Bronze 
SELECT * 
  FROM ggw_retail.customer_bronze
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Silver 
SELECT * 
  FROM ggw_retail.customer_silver
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check GOLD 
SELECT * 
  FROM ggw_retail.channel_customers_gold
;

-- COMMAND ----------

-- MAGIC %md ## 4. Copy in fourth set of records - delete

-- COMMAND ----------

-- MAGIC %fs cp abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/customer-4-delete.csv abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/in/

-- COMMAND ----------

-- MAGIC %md ### Final start of pipeline to pickup Delete

-- COMMAND ----------

-- Check Raw
REFRESH TABLE $db_name.customers_raw;
SELECT * 
  FROM ggw_retail.customers_raw
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Bronze 
SELECT * 
  FROM ggw_retail.customer_bronze
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Silver 
SELECT * 
  FROM ggw_retail.customer_silver
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Quarantine 
SELECT * 
  FROM ggw_retail.customer_silver_quarantine
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check GOLD 
SELECT * 
  FROM ggw_retail.channel_customers_gold
;

-- COMMAND ----------

-- MAGIC %md ## 98. Error Scenario - bad data 
-- MAGIC   
-- MAGIC The below error scenario occurs when bad data is sent into the DLT pipeline.
-- MAGIC Use the next step to copy in this file, start the DLT pipeline and watch the DLT UI Quality Metrics panel for errors!

-- COMMAND ----------

-- MAGIC %fs cp abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/customer-98-bad-data.csv abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/in/

-- COMMAND ----------

-- Check Quarantine 
SELECT * 
  FROM ggw_retail.customer_silver_quarantine
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- MAGIC %md ## 99. Error Scenario  
-- MAGIC   
-- MAGIC The below scenario is that a file has been sent to cloudFiles landing area, but has not landed into Bronze (yet).  
-- MAGIC Use the next step to copy in this file, but do not start dlt pipeline, so from a monitoring perspective the file is not processed!  
-- MAGIC View the dashboard and see missing row.

-- COMMAND ----------

-- MAGIC %fs cp abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/customer-99-missing-updates.csv abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_retail/data/in/

-- COMMAND ----------

dbutils.notebook.exit()

-- COMMAND ----------

-- MAGIC %md ## -1. Reset solution including csv files

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %fs rm -r /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- MAGIC %fs ls /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

DROP DATABASE $db_name CASCADE;

-- COMMAND ----------

-- MAGIC %md ## Delta Lake based source table  
-- MAGIC   
-- MAGIC This is a data model for the above csv input files.

-- COMMAND ----------

-- 0. Create customer table patterned on MySQL schema
-- DROP TABLE ggw_retail.customer;
CREATE TABLE ggw_retail.customer (
  -- id INTEGER NOT NULL GENERATED ALWAYS AS (row_number() OVER(PARTITION BY email ORDER BY first_name)),
  id INTEGER NOT NULL,
  first_name varchar(255) NOT NULL,
  last_name varchar(255) NOT NULL,
  email varchar(255) NOT NULL,
  active INTEGER GENERATED ALWAYS AS (1),
  update_dt timestamp GENERATED ALWAYS AS (now()),
  update_user varchar(128) GENERATED ALWAYS AS (current_user())
);

-- COMMAND ----------

-- 1. insert first records
INSERT INTO ggw_retail.customer
(id, first_name, last_name, email)
VALUES
    (1001, 'Glenn', 'Wiebe', 'ggwiebe@gmail.com'),
    (1002, 'Graeme', 'Wiebe', 'glenn@wiebes.net')
;

-- COMMAND ----------

-- 1.+ Select these first records & the dlt moved records
SELECT *
  FROM ggw_retail.customer
;

-- COMMAND ----------

-- 2. insert more records
INSERT INTO ggw_retail.customer
(id, first_name, last_name, email)
VALUES
    (1003, 'Dillon', 'Bostwick', 'dillon@databricks.com'),
    (1004, 'Franco', 'Patano', 'franco.patano@databricks.com')
;

-- COMMAND ----------

-- 1.+ Select these first records & the dlt moved records
SELECT *
  FROM ggw_retail.customer
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- 3. insert the update records
INSERT INTO ggw_retail.customer
(id, first_name, last_name, email)
VALUES
    (1002, 'Glenn', 'Wiebe', 'glenn@wiebes.net')
;

-- -- 3. update record
-- UPDATE ggw_retail.customer
--    SET first_name = 'Glenn'
--  WHERE id = 1002

-- COMMAND ----------

-- 1.+ Select these first records & the dlt moved records
SELECT *
  FROM ggw_retail.customer
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- 4. insert the Delete record
INSERT INTO ggw_retail.customer
(id, first_name, last_name, email, active)
VALUES
    (1002, 'Glenn', 'Wiebe', 'glenn@wiebes.net', 0)
;

-- -- 4.+ Delete the second Glenn record
-- UPDATE ggw_retail.customer
--  WHERE id = 1002
-- ;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED ggw_retail.customer_silver;
