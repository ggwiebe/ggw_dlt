# Databricks notebook source
# MAGIC %md # Get Data File

# COMMAND ----------

# MAGIC %md ## Config

# COMMAND ----------

dbutils.widgets.text("db_name", "ggw_ods")

# COMMAND ----------

# Get parameter from Job parm passed in
db_name = dbutils.widgets.get("db_name")
print("Using Database: {}".format(db_name))

# COMMAND ----------

temp_folder   = "/tmp/glenn.wiebe/"
storage_root  = "abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/"
ingest_folder = db_name + "/data/in/"
file_name     = "countries-aggregated.csv"

local_file_uri     = "file://" +  temp_folder + file_name
print('Get file to:  {}'.format(local_file_uri))
storage_folder_uri = storage_root + ingest_folder
print('Copy file to: {}'.format(storage_folder_uri))

# COMMAND ----------

# MAGIC %md ## Get Data File

# COMMAND ----------

# MAGIC %sh
# MAGIC rm /tmp/glenn.wiebe/countries-aggregated.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -P /tmp/glenn.wiebe https://raw.githubusercontent.com/datasets/covid-19/main/data/countries-aggregated.csv

# COMMAND ----------

print("Copying file from local {} to storage {}...".format(local_file_uri, storage_folder_uri))
dbutils.fs.cp(local_file_uri, storage_folder_uri)
