# Databricks notebook source
# MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

# COMMAND ----------

# MAGIC %run ./Load_Metadata

# COMMAND ----------

metadf = load_metadata_df()
display(metadf)

# COMMAND ----------

tablelist = metadf.select("Table_Name").distinct().rdd.map(lambda x : x[0]).collect()
print(tablelist)

# COMMAND ----------

def grab_files_for_a_table(table,stagingloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging', extrasloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/extras', segregatedloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated'):  
  fileloc = dbutils.fs.ls(stagingloc)
  filelist = list(map(lambda p: p.name, fileloc))

  filelistreq = list(filter(lambda p: '#' + table + '#' in p, filelist))
  filelistextra = list(filter(lambda p: '#' + table + '#' not in p, filelist))

  len(filelistreq)
  if filelistreq:
    for x in filelistreq:
      dbutils.fs.mv(stagingloc + '/' + x, segregatedloc + '/' + table + '/' + x)
  if filelistextra:
    for x in filelistextra:
      dbutils.fs.mv(stagingloc + '/' + x, extrasloc + '/' + table + '/' + x)    

# COMMAND ----------

for y in tablelist:
  grab_files_for_a_table(y)