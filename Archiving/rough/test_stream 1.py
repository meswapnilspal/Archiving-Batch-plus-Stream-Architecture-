# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.dtecuprodedaadl05.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dtecuprodedaadl05.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dtecuprodedaadl05.dfs.core.windows.net", "ea7dd103-75c8-40b7-af7d-0eb52c60889b")
spark.conf.set("fs.azure.account.oauth2.client.secret.dtecuprodedaadl05.dfs.core.windows.net", "A4rVH~xHrbDv69NgKC7hPvtc.8_x_iMq9p")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dtecuprodedaadl05.dfs.core.windows.net", "https://login.microsoftonline.com/8e61d5fe-7749-4e76-88ee-6d8799ae8143/oauth2/token")

# COMMAND ----------

dberchz1_df = spark.readStream.format("delta").option("ignoreDeletes", "true").load("abfss://anp@rlspocgen2.dfs.core.windows.net/stream_delta_tables/adl07/")

# COMMAND ----------

from pyspark.sql.functions import *
dberchz1_df = dberchz1_df.withColumn("insertedTS",current_timestamp())

# COMMAND ----------

saveloc = "abfss://anp@rlspocgen2.dfs.core.windows.net/stream_delta_tables/adl02"
streamQuery = dberchz1_df.writeStream.format("delta").option("checkpointLocation",f"{saveloc}_checkpoint").start(saveloc)

# COMMAND ----------

loadloc = "abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/"
saveloc = "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Stream_data/"
dbname = "cods"

tbl_list = ['qi6_dberchz1','qi6_dberchz2','qi6_dberchz3']

# COMMAND ----------

from pyspark.sql.functions import *

for i in tbl_list:
    loadloc1 = loadloc + i
    saveloc1 = saveloc + i
    tbl1 = (dbname + '.' + i)
    df = spark.readStream.format("delta").option("ignoreDeletes", "true").load(loadloc1)
    df = df.withColumn("insertedTS",current_timestamp())
    streamQuery = df.writeStream.format("delta").option("checkpointLocation",f"{saveloc1}_checkpoint").start(saveloc1)
    #print(loadloc + i)
    #print(saveloc + i)
    #print(dbname + '.' + i)
    

# COMMAND ----------

