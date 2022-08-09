# Databricks notebook source
# MAGIC %md ## Dynamically Read Constraints from Table  
# MAGIC   
# MAGIC Use the pipeline config setting:  
# MAGIC ```    "configuration": {  ```  
# MAGIC ```        "ggwdlt.pipeline.entityName": "Product",  ```
# MAGIC   
# MAGIC to set the specific entity on which you want to set constraints.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

try:
  spark.conf.get("ggwdlt.pipeline.entityName")
except:
  spark.conf.set("ggwdlt.pipeline.entityName", "Product")

inputData = spark.conf.get("ggwdlt.pipeline.entityName")
dataPath = f"abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_template/adventureworks/SalesLT.{inputData}/"

print(f"Target folder path: {dataPath}")

# COMMAND ----------

conf = spark.table("ggw_template.sources").filter(f"TableName = '{inputData}'").first()
dfconf = json.loads(conf.ReadOptions)
expectconf = json.loads(conf.Expectations)

print(f"data frame options: {dfconf}")
print(f"data frame options: {expectconf}")

# COMMAND ----------

df = (spark
      .read
      .options(**dfconf)
      .csv(dataPath)
    ) 

df.display()

# COMMAND ----------

import dlt
@dlt.table(name = f"DLT{inputData}",
  comment= f"{inputData} Table from RAW."
)
@dlt.expect_all(expectconf)


def clickstream_raw():
  return (df)

# COMMAND ----------


