# Databricks notebook source
# DBERCHZ2

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

df4 = df3.drop("rownum")
print(df4.columns)

# COMMAND ----------

from pyspark.sql.window import Window

windowspec = Window.partitionBy('MANDT','BELNR','BELZEILE').orderBy('EQUNR')
df5 = df4.withColumn('rownum',row_number().over(windowspec))
df6 = (df5.select('rownum','*').where('rownum = 1'))
df7 = (df5.select('rownum','*').where('rownum > 1'))

# COMMAND ----------

df6.write.format("delta").mode("append").save("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/ISU/dberchz2")

# COMMAND ----------

# DBERCHZ3

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.read.format('delta').load('abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/Streamed_data/qi6_dberchz3')

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

df4 = df3.drop("rownum")
print(df4.columns)

# COMMAND ----------

from pyspark.sql.window import Window

windowspec = Window.partitionBy('MANDT','BELNR','BELZEILE').orderBy('EQUNR')
df5 = df4.withColumn('rownum',row_number().over(windowspec))
df6 = (df5.select('rownum','*').where('rownum = 1'))
df7 = (df5.select('rownum','*').where('rownum > 1'))

# COMMAND ----------

df6.write.format("delta").mode("append").save("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/curated/ISU_archive/Replicate/ISU/dberchz3")