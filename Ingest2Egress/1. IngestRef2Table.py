# Databricks notebook source
# MAGIC %md # Get Reference File

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
file_name     = "Countries-Continents.csv"
table_name    = "Country"

local_file_uri     = "file://" +  temp_folder + file_name
print('Get file to:  {}'.format(local_file_uri))
storage_folder_uri = storage_root + ingest_folder
print('Copy file to: {}'.format(storage_folder_uri))

# COMMAND ----------

# MAGIC %md ## Get Data

# COMMAND ----------

# MAGIC %sh
# MAGIC rm /tmp/glenn.wiebe/Countries-Continents.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -P /tmp/glenn.wiebe https://raw.githubusercontent.com/dbouquin/IS_608/master/NanosatDB_munging/Countries-Continents.csv

# COMMAND ----------

print("Copying file from local {} to storage {}...".format(local_file_uri, storage_folder_uri))
dbutils.fs.cp(local_file_uri, storage_folder_uri)

# COMMAND ----------

# MAGIC %md ## Read Data

# COMMAND ----------

countries_df = (spark.read.format("csv") 
                          .option("header",True)
                          .option("infer_schema", True)
                          .load(storage_folder_uri+'/'+file_name)
               )

display(countries_df)

# COMMAND ----------

# MAGIC %md ## Write as Table

# COMMAND ----------

# Write Dataframe to table, overwriting if necessary
countries_df.write.mode("overwrite").saveAsTable(db_name+'.'+table_name)
