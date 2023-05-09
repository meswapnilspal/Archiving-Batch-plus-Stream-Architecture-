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

fileloc = dbutils.fs.ls(stagingloc)
filelist = list(map(lambda p: p.name, fileloc))

# COMMAND ----------

def grab_files_for_a_table(table,file_list,stagingloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging', segregatedloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated'):
  filelistreq = list(filter(lambda p: '#' + table + '#' in p, file_list))
  len(filelistreq)
  if filelistreq:
    for x in filelistreq:
      #print(stagingloc + '/' + x, segregatedloc + '/' + table + '/' + x)
      dbutils.fs.mv(stagingloc + '/' + x, segregatedloc + '/' + table + '/' + x)
  return(filelistreq)

# COMMAND ----------

for y in tablelist:
  filelistreq = grab_files_for_a_table(y,filelist)
  alldatafiles = alldatafiles + filelistreq

# COMMAND ----------

extrafiles = [item for item in filelist if item not in alldatafiles]

# COMMAND ----------

def move_extrafiles(extra_files,stagingloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging',extrasloc='abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/extras'):
  for x in extra_files:
    dbutils.fs.mv(stagingloc + '/' + x, extrasloc + '/' + x)

# COMMAND ----------

for y in extrafiles:
  move_extrafiles(y)

# COMMAND ----------

# dbutils.fs.cp("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/archive_data", "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging", True) 
# dbutils.fs.rm("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/DBERCHZ2", True)

# COMMAND ----------

# dbutils.fs.cp("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging", "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging_backup", True)
# dbutils.fs.rm("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/", True)

# COMMAND ----------

# dbutils.fs.rm("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/", True)
# dbutils.fs.rm("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging/", True)
# dbutils.fs.rm("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging_backup/", True)
# dbutils.fs.rm("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/extras/", True)