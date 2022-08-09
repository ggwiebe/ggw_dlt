# Databricks notebook source
# MAGIC %md # Ingest File to Table

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
table_name    = "country_daily_casecount"

local_file_uri     = "file://" +  temp_folder + file_name
print('Get file to:  {}'.format(local_file_uri))
storage_folder_uri = storage_root + ingest_folder
print('Copy file to: {}'.format(storage_folder_uri))

# COMMAND ----------

# MAGIC %md ## Get Data

# COMMAND ----------

# MAGIC %sh
# MAGIC rm /tmp/glenn.wiebe/countries-aggregated.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -P /tmp/glenn.wiebe https://raw.githubusercontent.com/datasets/covid-19/main/data/countries-aggregated.csv

# COMMAND ----------

print("Copying file from local {} to storage {}...".format(local_file_uri, storage_folder_uri))
dbutils.fs.cp(local_file_uri, storage_folder_uri)

# COMMAND ----------

# MAGIC %md ## Read Data

# COMMAND ----------

covid_schema = "Date Date, Country String, Confirmed Integer, Recovered Integer, Deaths Integer"

# COMMAND ----------

covid_df = (spark.read.format("csv")
                 .schema(covid_schema)
                 .option("header",True) 
                 .option("infer_schema", True)
                 .load(storage_folder_uri+'/'+file_name)
           )

display(covid_df)

# COMMAND ----------

# MAGIC %md ## Write as Table

# COMMAND ----------

# Write Dataframe to table, overwriting if necessary
covid_df.write.mode("overwrite").saveAsTable(db_name+'.'+table_name)
