# Databricks notebook source
import dlt

@dlt.table(
  name="GL_Total_Loan_Balances_z",
  comment="Combines historical and new loan data for unified rollup of loan balances & Z-Optimized",
  table_properties={
   "quality" : "gold",
   "pipelines.autoOptimize.zOrderCols" : "location_code"
  }
)
def total_loan_balances_z():
  return (
    spark.sql("""
      SELECT sum(revol_bal)  AS bal,
             addr_state      AS location_code
        FROM LIVE.SV_historical_txs
       GROUP BY addr_state
      UNION
      SELECT sum(balance)    AS bal,
             country_code    AS location_code
        FROM LIVE.SV_cleaned_new_txs
       GROUP BY country_code
    """)
  )



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Moved this to a Python Notebook!
# MAGIC 
# MAGIC -- CREATE LIVE TABLE GL_Total_Loan_Balances_z
# MAGIC -- COMMENT "Combines historical and new loan data for unified rollup of loan balances"
# MAGIC -- TBLPROPERTIES (
# MAGIC --   "quality" = "gold",
# MAGIC --   "pipelines.autoOptimize.zOrderCols" = "location_code"
# MAGIC -- )
# MAGIC -- AS 
# MAGIC -- SELECT sum(revol_bal)  AS bal,
# MAGIC --        addr_state      AS location_code
# MAGIC --   FROM LIVE.SV_historical_txs
# MAGIC --  GROUP BY addr_state
# MAGIC -- UNION
# MAGIC -- SELECT sum(balance)    AS bal,
# MAGIC --        country_code    AS location_code
# MAGIC --   FROM LIVE.SV_cleaned_new_txs
# MAGIC --  GROUP BY country_code
