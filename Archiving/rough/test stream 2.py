# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dtecuprodedaadl05.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dtecuprodedaadl05.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dtecuprodedaadl05.dfs.core.windows.net", "ea7dd103-75c8-40b7-af7d-0eb52c60889b")
spark.conf.set("fs.azure.account.oauth2.client.secret.dtecuprodedaadl05.dfs.core.windows.net", "A4rVH~xHrbDv69NgKC7hPvtc.8_x_iMq9p")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dtecuprodedaadl05.dfs.core.windows.net", "https://login.microsoftonline.com/8e61d5fe-7749-4e76-88ee-6d8799ae8143/oauth2/token")

# COMMAND ----------

dberchz1_df = spark.readStream.format("delta").option("ignoreChanges", "true").load("abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz1")
dberchz1_df = dberchz1_df.withColumn("insertedTS",current_timestamp())

# COMMAND ----------

saveloc = "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Streamed_data/qi6_dberchz1"
streamQuery1 = dberchz1_df.writeStream.format("delta").option("checkpointLocation",f"{saveloc}_checkpoint").start(saveloc)

# COMMAND ----------



# COMMAND ----------



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



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists edw.dberchz2_poc;
# MAGIC create table if not exists edw.dberchz2_poc using delta location 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Streamed_data/qi6_dberchz2';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from edw.dberchz2_poc

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.read.format('delta').load('abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Streamed_data/qi6_dberchz2')

# COMMAND ----------

cols = [x for x in df.columns if x != 'insertedTS']
print(cols)

# COMMAND ----------

from pyspark.sql.window import Window
windowspec = Window.partitionBy(cols).orderBy(col('insertedTS').desc())
df2 = df.withColumn('rownum',row_number().over(windowspec))

# COMMAND ----------

df3 = (df2.select('*').where('rownum = 1'))

# COMMAND ----------

df3.columns

# COMMAND ----------

df4 = df3.drop("rownum")

# COMMAND ----------

df4.columns

# COMMAND ----------

# display(df2.limit(1000))

# COMMAND ----------

from pyspark.sql.window import Window

windowspec = Window.partitionBy('MANDT','BELNR','BELZEILE').orderBy('EQUNR')
df5 = df4.withColumn('rownum',row_number().over(windowspec))
df6 = (df5.select('rownum','*').where('rownum = 1'))
df7 = (df5.select('rownum','*').where('rownum > 1'))

# COMMAND ----------

df6.count()

# COMMAND ----------

df7.count()

# COMMAND ----------

# %sql

# drop table if exists edw.dberchz1_poc;

# COMMAND ----------

# dbutils.fs.rm('abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Streamed_data/')

# COMMAND ----------

