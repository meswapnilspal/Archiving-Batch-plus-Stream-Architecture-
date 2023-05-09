# Databricks notebook source
dup_test = spark.read.format('delta').load('abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz2')

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
windowspec = Window.partitionBy(temp_source.columns).orderBy(col('op_timestamp').desc())
df2 = dup_test.withColumn('rownum',row_number().over(windowspec))

df2.select('*').where('rownum > 1').count()

# COMMAND ----------

display(df2.select('*').where('rownum > 1'))

# COMMAND ----------

dedup_test = spark.read.format('delta').load('abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz2')

# COMMAND ----------

from pyspark.sql.window import Window

windowspec = Window.partitionBy('MANDT','BELNR','BELZEILE').orderBy('EQUNR')
df5 = dedup_test.withColumn('rownum',row_number().over(windowspec))
df6 = (df5.select('rownum','*').where('rownum = 1'))
df7 = (df5.select('rownum','*').where('rownum > 1'))

# COMMAND ----------

print(df6.count())
print(df7.count())

# COMMAND ----------

