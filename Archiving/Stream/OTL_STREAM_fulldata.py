# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.dtecuprodedaadl05.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dtecuprodedaadl05.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dtecuprodedaadl05.dfs.core.windows.net", "ea7dd103-75c8-40b7-af7d-0eb52c60889b")
spark.conf.set("fs.azure.account.oauth2.client.secret.dtecuprodedaadl05.dfs.core.windows.net", "A4rVH~xHrbDv69NgKC7hPvtc.8_x_iMq9p")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dtecuprodedaadl05.dfs.core.windows.net", "https://login.microsoftonline.com/8e61d5fe-7749-4e76-88ee-6d8799ae8143/oauth2/token")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dberchz2_df = spark.readStream.format("delta").option("ignoreChanges", "true").load("abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz2")
dberchz2_df = dberchz2_df.withColumn("insertedTS",current_timestamp())

# COMMAND ----------

saveloc = "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Streamed_data/qi6_dberchz2"
streamQuery2 = dberchz2_df.writeStream.format("delta").option("checkpointLocation",f"{saveloc}_checkpoint").start(saveloc)

# COMMAND ----------

dberchz3_df = spark.readStream.format("delta").option("ignoreChanges", "true").load("abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz3")
dberchz3_df = dberchz3_df.withColumn("insertedTS",current_timestamp())

# COMMAND ----------

saveloc = "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Streamed_data/qi6_dberchz3"
streamQuery3 = dberchz3_df.writeStream.format("delta").option("checkpointLocation",f"{saveloc}_checkpoint").start(saveloc)

# COMMAND ----------

# rough
df_test = spark.read.format('delta').load("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Streamed_data/qi6_dberchz2")
df_test.count()



# COMMAND ----------

