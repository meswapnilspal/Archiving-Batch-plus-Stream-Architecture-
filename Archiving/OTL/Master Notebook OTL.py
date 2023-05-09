# Databricks notebook source
# MAGIC %run ./Load_Metadata

# COMMAND ----------

metadf = load_metadata_df()
display(metadf)

# COMMAND ----------

tablelist = metadf.select("Table_Name").distinct().rdd.map(lambda x : x[0]).collect()
print(tablelist)

# COMMAND ----------

tablelist = ['FKKMAKT', 'ERDO', 'DFKKOP', 'EMDUSDRQITEM', 'EABLG', 'DBERCHZ3', 'DBERCHV', 'DFKKZPE', 'EMDUSDRQINPUT', 'DFKKOPW', 'ETTIFN', 'DFKKZPT', 'ERCH', 'FKKMAKO', 'BCONT', 'DFKKOPK', 'EABL', 'DFKKOPCOLL', 'DFKKZP', 'ETTIFB', 'DBERDLB', 'EMDUSDRQRESULT', 'CDPOS', 'ERCHO', 'DFKKLOCKSH', 'EMDUSDRQHEAD', 'CDHDR', 'DBERCHZ2', 'DFKKOPKC', 'FKKMACTIVITIES', 'DBERDL', 'DFKKZS', 'DFKKCOLLH', 'DFKKCOLL', 'FKKMAZE', 'DFKKZK', 'DFKKLOCKS', 'ERDK', 'ERDB', 'DFKKOPKX', 'DFKKZV', 'ERCHC', 'DFKKKO']

# COMMAND ----------

for tablename in tablelist:
    dbutils.notebook.run("/EDA/Data Engineer/POD_7_Dev/Archiving/OTL/Dynamic Table Creation Using MetaTable", 1200, { "table_name" : tablename , "db_name" : "edw" , "curated_location" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/archive/FINAL" , "curated_location_err" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/archive/ERROR" })
    dbutils.notebook.run("/EDA/Data Engineer/POD_7_Dev/Archiving/OTL/Data Prepartion", 1200, { "table_name" : tablename , "staging_location" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated" , "curated_location" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/archive/FINAL" , "curated_location_err" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/archive/ERROR" })

# COMMAND ----------



# COMMAND ----------

tablelist = [ 'ERDB', 'DFKKOPKX', 'DFKKZV', 'ERCHC', 'DFKKKO']

# COMMAND ----------

for tablename in tablelist:
    dbutils.notebook.run("/EDA/Data Engineer/POD_7_Dev/Archiving/OTL/Dynamic Table Creation Using MetaTable", 1200, { "table_name" : tablename , "db_name" : "edw" , "curated_location" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/archive/FINAL" , "curated_location_err" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/archive/ERROR" })
    dbutils.notebook.run("/EDA/Data Engineer/POD_7_Dev/Archiving/OTL/Data Prepartion", 1200, { "table_name" : tablename , "staging_location" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated" , "curated_location" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/archive/FINAL" , "curated_location_err" : "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/archive/ERROR" })

# COMMAND ----------

