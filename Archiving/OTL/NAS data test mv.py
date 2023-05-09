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

# FOR TESTING

stagingloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging'
segregatedloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated'

fileloc = dbutils.fs.ls(stagingloc)
filelist = list(map(lambda p: p.name, fileloc))

filelistreq = list(filter(lambda p: '#DBERCHZ1#' in p, filelist))
len(filelistreq)

# COMMAND ----------

def grab_files_for_a_table(table,stagingloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging', segregatedloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated'):  
  fileloc = dbutils.fs.ls(stagingloc)
  filelist = list(map(lambda p: p.name, fileloc))

  filelistreq = list(filter(lambda p: '#' + table + '#' in p, filelist))
  len(filelistreq)
  if filelistreq:
    for x in filelistreq:
      #print(stagingloc + '/' + x, segregatedloc + '/' + table + '/' + x)
      dbutils.fs.mv(stagingloc + '/' + x, segregatedloc + '/' + table + '/' + x)

# COMMAND ----------

# dbutils.fs.cp("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/archive_data", "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging", True) 
# dbutils.fs.rm("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/DBERCHZ2", True)

# COMMAND ----------

# dbutils.fs.cp("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging", "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging_backup", True)
# dbutils.fs.rm("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/", True)

# COMMAND ----------

def grab_files_for_a_table_extra(table,stagingloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging', extrasloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/extras'):  
  fileloc = dbutils.fs.ls(stagingloc)
  #print(fileloc)
  filelist = list(map(lambda p: p.name, fileloc))

  filelistreq = list(filter(lambda p: '#' + table + '#' not in p, filelist))
  #print(filelistreq)
  len(filelistreq)
  if filelistreq:
    for x in filelistreq:
      dbutils.fs.mv(stagingloc + '/' + x, extrasloc + '/' + table + '/' + x)

# COMMAND ----------

for y in tablelist:
  grab_files_for_a_table_extra(y)

# COMMAND ----------

def grab_files_for_a_table(table,stagingloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/staging', extrasloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/extras', segregatedloc = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated'):  
  fileloc = dbutils.fs.ls(stagingloc)
  #print(fileloc)
  filelist = list(map(lambda p: p.name, fileloc))

  filelistreq = list(filter(lambda p: '#' + table + '#' in p, filelist))
  filelistextra = list(filter(lambda p: '#' + table + '#' not in p, filelist))
  #print(filelistreq)
  len(filelistreq)
  if filelistreq:
    for x in filelistreq:
      dbutils.fs.mv(stagingloc + '/' + x, segregatedloc + '/' + table + '/' + x)
  if filelistextra:
    for x in filelistextra:
      dbutils.fs.mv(stagingloc + '/' + x, extrasloc + '/' + table + '/' + x)    

# COMMAND ----------

segregatedlocCDPOS = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/CDPOS/'

dfCDPOS = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter","|").load(segregatedlocCDPOS)

display (dfCDPOS)

# COMMAND ----------

print(dfCDPOS.count()) 

# COMMAND ----------

from pyspark.sql.functions import col, substring
df3=dfCDPOS.withColumn('CHANGENR', col('CDPOS-CHANGENR').substr(1, 4))
display(df3)


# COMMAND ----------

dfCDPOS.select("CDPOS-CHANGENR",substring("CDPOS-CHANGENR",1,4)).show()