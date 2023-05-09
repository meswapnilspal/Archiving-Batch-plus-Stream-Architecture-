# Databricks notebook source
for file in dbutils.fs.ls("abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_Extraction/"):
  print(file.name)

# COMMAND ----------

base = "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_Extraction/"

# COMMAND ----------

ERDO_path = 	base +	"QI6#300#000008_IS-U_EE20_0000#ERDO#20220510#071629#0.CSV"
DBERDU_path = 	base +	"QI6#300#000008_IS-U_EE22_0000#DBERDU#20220510#071629#0.CSV"
EITERDK_path = 	base +	"QI6#300#000008_IS-U_EE22_0000#EITERDK#20220510#071629#0.CSV"
ERDB_path = 	base +	"QI6#300#000008_IS-U_EE22_0000#ERDB#20220510#071629#0.CSV"
ERDK_path = 	base +	"QI6#300#000008_IS-U_EE22_0000#ERDK#20220510#071629#0.CSV"


# COMMAND ----------

ERDO_df = spark.read.option("delimiter", "|").option("header","true").option("inferSchema","true").csv(ERDO_path)
DBERDU_df = spark.read.option("delimiter", "|").option("header","true").option("inferSchema","true").csv(DBERDU_path)
EITERDK_df = spark.read.option("delimiter", "|").option("header","true").option("inferSchema","true").csv(EITERDK_path)
ERDB_df = spark.read.option("delimiter", "|").option("header","true").option("inferSchema","true").csv(ERDB_path)
ERDK_df = spark.read.option("delimiter", "|").option("header","true").option("inferSchema","true").csv(ERDK_path)


# COMMAND ----------

display(ERDO_df)

# COMMAND ----------

display(DBERDU_df)

# COMMAND ----------

display(EITERDK_df)

# COMMAND ----------

display(ERDB_df)

# COMMAND ----------

display(ERDK_df)

# COMMAND ----------

# ERDO_df.createOrReplaceTempView("ERDO_temp")
# DBERDU_df.createOrReplaceTempView("DBERDU_temp")
# EITERDK_df.createOrReplaceTempView("EITERDK_temp")
# ERDB_df.createOrReplaceTempView("ERDB_temp")
# ERDK_df.createOrReplaceTempView("ERDK_temp")

# COMMAND ----------

base2 = 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_Extraction_delta/'

# COMMAND ----------

ERDO_df.write.format('delta').mode('overwrite').save(base2 + 'ERDO')
DBERDU_df.write.format('delta').mode('overwrite').save(base2 + 'DBERDU')
EITERDK_df.write.format('delta').mode('overwrite').save(base2 + 'EITERDK')
ERDB_df.write.format('delta').mode('overwrite').save(base2 + 'ERDB')
ERDK_df.write.format('delta').mode('overwrite').save(base2 + 'ERDK')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS edw.ERDO_temp
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_Extraction_delta/ERDO' ;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS edw.DBERDU_temp
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_Extraction_delta/DBERDU' ;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS edw.EITERDK_temp
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_Extraction_delta/EITERDK' ;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS edw.ERDB_temp
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_Extraction_delta/ERDB' ;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS edw.ERDK_temp
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/PBS_Extraction_delta/ERDK' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edw.ERDK_temp

# COMMAND ----------

