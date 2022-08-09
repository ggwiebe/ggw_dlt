# Databricks notebook source
# MAGIC %md # Data Egress - Publish Data as CSV File

# COMMAND ----------

# MAGIC %md ## Config

# COMMAND ----------

dbutils.widgets.text("db_name", "ggw_ods")

# COMMAND ----------

# Get parameter from Job parm passed in
db_name = dbutils.widgets.get("db_name")
print("Using Database: {}".format(db_name))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format
import datetime

storage_root  = "abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/"
out_folder    = db_name + "/data/out/"
out_date      = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
separator     = ','
header        = 'true'
compress_type = 'gzip'

folder_out_uri = storage_root + out_folder + "covid-out/" + out_date
print("Output folder uri: {}".format(folder_out_uri))

file_out_uri   = storage_root + out_folder + "covid-publish/" + out_date + '.csv'
print("Publish file uri: {}".format(file_out_uri))

# COMMAND ----------

# MAGIC %md ## Read Data

# COMMAND ----------

cdeath_df = ( spark.sql("""
                         SELECT *
                           FROM {}.ContinentMaxDeaths
                      """.format(db_name))
          )
# display(cust_df)

# COMMAND ----------

# MAGIC %md ## Write CSV File

# COMMAND ----------

( cdeath_df.coalesce(1).write
                       .format("csv")
                       .option("header", header)
                       .option("delimiter", separator)
#                        .option("compression", compress_type)
                       .save(folder_out_uri)
)

# COMMAND ----------

# MAGIC %md ## Publish to location

# COMMAND ----------

filelist = dbutils.fs.ls(folder_out_uri)
# display(filelist)

for f_name in filelist:
     if f_name.name.endswith('.csv'):
         print('found csv file: {}/{}'.format(folder_out_uri,f_name.name))
         print('Publishing csv file to: {}'.format(file_out_uri))
         dbutils.fs.cp(f_name.path,file_out_uri)
