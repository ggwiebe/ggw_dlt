# Databricks notebook source
# -- BRONZE - Read raw streaming file reader for "new" customer records
# CREATE INCREMENTAL LIVE TABLE customer_bronze
# TBLPROPERTIES ("quality" = "bronze")
# COMMENT "New customer data incrementally ingested from cloud object storage landing zone"
# AS 
# SELECT *
#   FROM cloud_files('/Users/glenn.wiebe@databricks.com/ggw_retail/data/in/', 'csv')
# --   OPTIONS (header="true", inferSchema="true")

# COMMAND ----------

import dlt

@dlt.table(
  name="customer_bronze",
  comment="New customer data incrementally ingested from cloud object storage landing zone",
  table_properties={
   "quality" : "bronze"
  }
)
def customer_raw_csv():
  return (
    spark.readStream.format('cloudFiles')
      .option('cloudFiles.format', 'csv')
      .option('header', 'true')
      .schema('id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string')
      .load('/Users/glenn.wiebe@databricks.com/ggw_retail/data/in/')
  )

