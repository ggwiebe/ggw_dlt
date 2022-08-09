-- Databricks notebook source
-- Get head of raw transactions
SELECT *
  FROM ggw_loans.bz_raw_txs
LIMIT 100
;

-- COMMAND ----------

-- Get details of count of raw transactions
SELECT COUNT(*)
  FROM ggw_loans.bz_raw_txs
;

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/lending-club-loan-stats/

-- COMMAND ----------

-- Get details of raw loan stats
-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
-- location on dbfs: /databricks-datasets/lending-club-loan-stats/LoanStats_2018Q2.csv
CREATE TEMPORARY VIEW raw_reference_loan_stats
 USING CSV
OPTIONS (path "/databricks-datasets/lending-club-loan-stats/LoanStats_2018Q2.csv", header "true", mode "FAILFAST")
;

-- Get details of bronze loan stats
SELECT *
  FROM raw_reference_loan_stats
;

-- Get details of bronze loan stats
-- SELECT *
--   FROM ggw_loans.bz_reference_loan_stats
-- ;

-- COMMAND ----------

-- Get details of loan stats
SELECT a.*
  FROM      ggw_loans.BZ_reference_loan_stats a
 INNER JOIN ggw_loans.ref_accounting_treatment b 
            USING (id)

-- COMMAND ----------

-- Get details for silver with matching Account Treatment ID
  SELECT txs.*, 
        rat.id AS accounting_treatment 
   FROM ggw_loans.BZ_raw_txs txs
  INNER JOIN ggw_loans.ref_accounting_treatment rat 
          ON txs.accounting_treatment_id = rat.id
;

-- COMMAND ----------

-- Get details from Gold Load Balance Table #1
SELECT location_code,
       bal
  FROM ggw_loans.gl_total_loan_balances
 ORDER BY bal DESC
;

-- COMMAND ----------

-- Describe 
-- DESCRIBE EXTENDED ggw_loans.gl_total_loan_balances;
DESCRIBE EXTENDED ggw_loans.gl_total_loan_balances_z;

-- COMMAND ----------

-- Get details from Gold Load Balance Table #2
SELECT location_code,
       bal
  FROM ggw_loans.gl_total_loan_balances_z
 ORDER BY bal DESC
;

-- COMMAND ----------

-- Get loan stats for only those with matching accounting treatment id
SELECT a.*
  FROM      ggw_loans.bz_reference_loan_stats a
 INNER JOIN ggw_loans.ref_accounting_treatment b 
            USING (id)
