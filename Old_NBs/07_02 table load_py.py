# Databricks notebook source
# MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

# COMMAND ----------

dbutils.widgets.text("Table_name", "", "Source Table Name")
tbl_nm = dbutils.widgets.get("Table_name")
source_path = "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_QI6/"
# target_path = "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_QI6_Target/"
target_tbl = "common." + tbl_nm

files = dbutils.fs.ls(source_path)
file_paths = []
file_count = 0

for file in dbutils.fs.ls(source_path):
  if  tbl_nm + "#" in file.path:
    file_path = file.path
    file_count =file_count+1
    file_paths.append(file_path)

# COMMAND ----------

tgt_df = spark.read.format("csv").option("sep", ";").option("header", "true").load(file_paths)
new_column_name_list= list(map(lambda x: x.replace(tbl_nm+"-", ""), tgt_df.columns))
tgt_df_new = tgt_df.toDF(*new_column_name_list)
tgt_df_new.write.format("delta").mode("append").saveAsTable(target_tbl)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  common.DBERCHZ3