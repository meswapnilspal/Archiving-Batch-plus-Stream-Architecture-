# Databricks notebook source
segregatedlocCDPOS = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/CDPOS/'
dfCDPOS = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter","|").load(segregatedlocCDPOS)
display (dfCDPOS)

# COMMAND ----------

print(dfCDPOS.count()) 

# COMMAND ----------

from pyspark.sql.functions import col, substring
df=dfCDPOS.withColumn('CHANGENR', col('CDPOS-CHANGENR').substr(1, 3))
display(df)


# COMMAND ----------


df.groupBy("CHANGENR").count().show()


# COMMAND ----------


display(df.groupBy("CHANGENR").count())

# COMMAND ----------

# MAGIC %md
# MAGIC New Table

# COMMAND ----------


df.groupBy("CHANGENR").count().show()

# COMMAND ----------

segregatedlocDFKKOPK = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/raw/ISU_archive/segregated/DFKKOPK/'
dfDFKKOPK = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter","|").load(segregatedlocDFKKOPK)
display (dfDFKKOPK)

# COMMAND ----------

print(dfDFKKOPK.count()) 

# COMMAND ----------

from pyspark.sql.functions import col, substring
df2=dfDFKKOPK.withColumn('OPBEL', col('DFKKOPK-OPBEL').substr(1, 4))
display(df2)


# COMMAND ----------

df2.groupBy("OPBEL").count().show()