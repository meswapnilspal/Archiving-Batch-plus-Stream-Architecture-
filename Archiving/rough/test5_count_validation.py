# Databricks notebook source
# MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

# COMMAND ----------

# dbutils.fs.cp("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/archive_data", "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging", True) 
# dbutils.fs.rm("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/DBERCHZ2", True)

# COMMAND ----------

basepath = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/'
dbutils.fs.ls(basepath)

# COMMAND ----------

ctr = 0
for x in dbutils.fs.ls(basepath):
  for y in dbutils.fs.ls(basepath + x.name):
    print(y.name)
    ctr = ctr + 1
    print(ctr)
  
print('---------------------------')
print(ctr)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edw.isu_ERDO_archive;

# COMMAND ----------

