# Databricks notebook source
# MAGIC %md ## Backup all metadata for ggw_retail  
# MAGIC   
# MAGIC Just in case!

# COMMAND ----------

counter = 0
ddls = []

# for db_name, *_ in spark.catalog.listDatabases():
#  if db_name != 'ggw_retail': continue
db_name = 'ggw_retail'

for table_name, *_, is_tmp in spark.catalog.listTables(db_name):
 if is_tmp: 
   continue
 try:
   table_ddl = spark.sql(f'SHOW CREATE TABLE {db_name}.{table_name}').first()[0]
   table_ddl = table_ddl.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
   ddls.append(table_ddl)
 except Exception as e:
   print(f'Problem dumping DDL for {db_name}.{table_name}: {e}')
 counter += 1
 if counter % 100 == 0: print(f'Exported {counter} tables')

# dbutils.fs.put('/tmp/metastore-export.sql',
#               ("-- Databricks notebook source\n" +
#               ("\n-- COMMAND ----------\n".join(ddls))),
#               overwrite=True)

# print(ddls)

# COMMAND ----------

print(ddls)

# COMMAND ----------

for delta_table in [“table_1”, “table_100”]:
    # better run in bounded parallel thread pool
    spark.sql(f”CREATE TABLE  
         delta.`/mnt/primary-region/{delta_table}`
         DEEP CLONE 
         delta.`/mnt/secondary-region/{delta_table}`”)
