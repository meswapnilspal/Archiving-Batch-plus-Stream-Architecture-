# Databricks notebook source
df = spark.read.format('delta').load('abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Streamed_data/qi6_dberchz2')

# COMMAND ----------

cols = [x for x in df.columns if x != 'insertedTS']
print(cols)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
windowspec = Window.partitionBy(cols).orderBy(col('insertedTS').desc())
df2 = df.withColumn('rownum',row_number().over(windowspec))

# COMMAND ----------

df2.select('*').where('rownum = 1').count()

# COMMAND ----------

# display(df2.select('*').where('rownum > 1'))

# COMMAND ----------

415627289 -- source

415630645 -- after duplicate(full) removal 

415624979 -- after de-duplicate(partial) removal
