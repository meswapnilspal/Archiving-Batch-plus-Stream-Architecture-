# Databricks notebook source
# MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

# COMMAND ----------

# historical data table
# common.dberchz2

# current data table
# isu_archive.dberchz2

# merged view
# common.DBERCHZ2_V

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS common.DBERCHZ2_V AS
# MAGIC Select *,'HISTORICAL' AS EPOCH_FLAG from common.dberchz2
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from isu_archive.dberchz2

# COMMAND ----------

# MAGIC %sql
# MAGIC select EPOCH_FLAG,count(*) from common.DBERCHZ2_V group by EPOCH_FLAG

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from common.DBERCHZ2_V

# COMMAND ----------

