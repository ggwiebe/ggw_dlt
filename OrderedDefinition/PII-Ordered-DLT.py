# Databricks notebook source
# MAGIC %md # Ordered Table Generation/Definition  
# MAGIC   
# MAGIC The following pattern or DLT function and root invocation allows one to define and generate the DLT tables in order.  
# MAGIC That will ensure pre-requisites that must be in place first are enabled.

# COMMAND ----------

# MAGIC %md ## 0. Setup for Scenario  
# MAGIC   
# MAGIC ```
# MAGIC %sql
# MAGIC CREATE TABLE ggw_retail.Clean(id int, description string)
# MAGIC ;
# MAGIC CREATE TABLE ggw_retail.quarantine(id int, description2 string)
# MAGIC ;
# MAGIC CREATE TABLE ggw_retail.clean_processed(id int, description3 string)
# MAGIC ;
# MAGIC ```

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

def generate_tables(my_table, table_desc):
  @dlt.table(
    name=(my_table + '_dlt'),
    comment="PII Table - {}".format(table_desc)
  )
  def create_table():
    # Here the entire dlt definition is parameterized, so no need to have unique return statements;
    # If you need different queries, or constraints, etc. use the next approach below.
    return spark.sql('SELECT * FROM ggw_retail.{}'.format(my_table))


generate_tables("Clean", "Clean Table")
generate_tables("quarantine", "Quarantine Table")
generate_tables("clean_processed", "Clean & Processed Table")

# COMMAND ----------

# MAGIC %md ## Not completely dynamic case  
# MAGIC   
# MAGIC ```
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC 
# MAGIC def generate_tables(my_table, table_desc):
# MAGIC   @dlt.table(
# MAGIC     name=(my_table + '_dlt'),
# MAGIC     comment="PII Table - {}".format(table_desc)
# MAGIC   )
# MAGIC   def create_table():
# MAGIC 
# MAGIC #     match my_table:
# MAGIC #       case "Clean":
# MAGIC     if (my_table == "Clean"):
# MAGIC         return spark.sql('SELECT * FROM ggw_retail.{}'.format(my_table))
# MAGIC 
# MAGIC #       case "quarantine":
# MAGIC     elif (my_table == "quarantine"):
# MAGIC         return spark.sql('SELECT * FROM ggw_retail.{}'.format(my_table))
# MAGIC 
# MAGIC #       case "clean_processed":
# MAGIC     elif (my_table == "clean_processed"):
# MAGIC         return spark.sql('SELECT * FROM ggw_retail.{}'.format(my_table))
# MAGIC 
# MAGIC 
# MAGIC generate_tables("Clean", "Clean Table")
# MAGIC generate_tables("quarantine", "Quarantine Table")
# MAGIC generate_tables("clean_processed", "Clean & Processed Table")
# MAGIC ```
