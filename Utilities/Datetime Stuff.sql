-- Databricks notebook source
-- MAGIC %md # Fun with Dates  
-- MAGIC   
-- MAGIC In Canada in 1975 Daylight Savings began on Apr 27, 1975  
-- MAGIC In Europe in 1975 Daylight Savings Was not observed!!! Europe harmonized Summer Time in 1981  
-- MAGIC   
-- MAGIC In Canada in 1992 Daylight Savings began on Apr 05, 1992  
-- MAGIC In Europe in 1992 Daylight Savings began on Mar 29, 1992  
-- MAGIC   
-- MAGIC In Canada in 2022 Daylight Savings began on Mar 13, 2022  
-- MAGIC In Europe in 2022 Daylight Savings began on Mar 27, 2022  
-- MAGIC   
-- MAGIC In Montreal on April 17, 1975 Montreal is on Standard Time (i.e. UTC -05:00)  
-- MAGIC In Montreal on April 17, 1992 Montreal is on Daylight Saving Time (i.e. UTC -04:00)  
-- MAGIC In Montreal on April 17, 2022 Montreal is on Daylight Saving Time (i.e. UTC -04:00)  

-- COMMAND ----------

-- Legacy Daylight Savings time (Europe & North America on same schedule)
-- In Canada Daylight Savings began on Apr 27, 1975
-- on 1975-04-17, Montréal is Standard Time
SELECT TO_TIMESTAMP('1975-04-17 04:00:00') AS `TS_1974-04-17T04_UTC`, -- April 17, 04:00 UTC
       from_utc_timestamp(
         TO_TIMESTAMP('1975-04-17 04:00:00'),
         'America/Montreal'
       ) AS `fromUTC_1974-04-17T04_to_EST`,                           -- April 17, 04:00 UTC --> April 16 23:00 Montreal/EST, i.e. Montreal/EST = UTC-05:00
       to_utc_timestamp(
         TO_TIMESTAMP('1975-04-17 00:00:00'),
         'America/Montreal'
       ) AS `toUTC_1974-04-17T00_fromEST`                             -- April 17, 00:00 Montreal/EST (UTC-05:00) --> April 17, 05:00 UTC

-- COMMAND ----------

-- New Daylight Savings time (Europe & North America on different schedule)
-- In Canada in 2022 Daylight Savings began on Mar 13, 2022
-- In Europe in 2022 Daylight Savings began on Mar 27, 2022
-- on 2022-04-17, Montréal is Daylight Savings Time / EDT ie. UTC - 04:00

SELECT TO_TIMESTAMP('2022-04-17 04:00:00') AS `TS_2022-04-17_UTC`, -- April 17, 04:00 UTC
       from_utc_timestamp(
         TO_TIMESTAMP('2022-04-17 04:00:00'),
         'America/Montreal'
       ) AS `fromUTC_2022-04-17_Montreal`,                         -- April 17, 04:00 UTC --> April 17 00:00 Montreal/EDT i.e. Montreal = UTC - 04:00
       to_utc_timestamp(
         TO_TIMESTAMP('2022-04-17 00:00:00'),
         'America/Montreal'
       ) AS `toUTC_2022-04-17_Montreal`                            -- April 17, 00:00 Montreal/EDT (UTC - 04:00) --> April 17, 04:00 UTC
