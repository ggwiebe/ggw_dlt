-- Databricks notebook source
drop table if exists ggw_template.sources

-- COMMAND ----------

create table if not exists ggw_template.sources
(
TableName string,
ReadOptions string,
Expectations string
)

-- COMMAND ----------

truncate table ggw_template.sources

-- COMMAND ----------

insert into ggw_template.sources
select 'Product' ,'{"Header":"True", "Sep": ",", "inferSchema": "True"}', '{"Valid Price": "ListPrice < 10000", "Missing ModelName": "ModelName IS NOT NULL"}'

-- COMMAND ----------

insert into ggw_template.sources
select 'Address' ,'{"Header":"True", "Sep": ",", "inferSchema": "True"}', '{"Valid Address": "AddressLine1 IS NOT NULL"}'

-- COMMAND ----------

SELECT *
  FROM ggw_template.sources
;

-- COMMAND ----------


