# Databricks notebook source
#Step 1 : Create Dataframe on CT dataset

# File location and type
file_location = "/FileStore/tables/demochangesheet_ct-2.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dfupdate = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(dfupdate)

# COMMAND ----------

# Step 2 : Create directory to store Delta data in Filestore
dbutils.fs.mkdirs("/FileStore/tables/target/delta2")

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/target/delta2

# COMMAND ----------

#Step 3: Create delta table on Dataframe "Initial load / Initial table creation"
# No need to repeat it again for CT Dataframes
dfupdate.write.format("delta").mode("overwrite").save("/FileStore/tables/target/delta2/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM DELTA.`/FileStore/tables/target/delta2/`

# COMMAND ----------

#Step 4 : Merge logic 

from delta.tables import *

deltaTableTarget = DeltaTable.forPath(spark, '/FileStore/tables/target/delta2/')
deltaTableUpdates = dfupdate

dfUpdates = deltaTableUpdates

deltaTableTarget.alias('target') \
  .merge(
    dfUpdates.alias('update'),
    'target.MANDT = update.MANDT AND target.BELNR = update.BELNR AND target.BELZEILE = update.BELZEILE'
  ) \
  .whenMatchedUpdate(set =
    {
      "MANDT"	: "update.MANDT",
      "BELNR"	: "update.BELNR",
      "BELZEILE" : "update.BELZEILE",	
      "EQUNR"	: "update.EQUNR",
      "GERAET" : "update.GERAET",	
      "MATNR"	: "update.MATNR",
      "ZWNUMMER" : "update.ZWNUMMER",
      "INDEXNR" : "update.INDEXNR",
      "ABLESGR" : "update.ABLESGR",	
      "ABLESGRV" : "update.ABLESGRV",	
      "ATIM" : "update.ATIM",	
      "ATIMVA" : "update.ATIMVA",	
      "ADATMAX" : "update.ADATMAX",	
      "ATIMMAX" : "update.ATIMMAX",	
      "THGDATUM" : "update.THGDATUM",	
      "ZUORDDAT" : "update.ZUORDDAT",	
      "REGRELSORT" : "update.REGRELSORT",	
      "ABLBELNR" : "update.ABLBELNR",	
      "LOGIKNR" : "update.LOGIKNR",	
      "LOGIKZW" : "update.LOGIKZW",	
      "ISTABLART" : "update.ISTABLART",	
      "ISTABLARTVA" : "update.ISTABLARTVA",	
      "EXTPKZ" : "update.EXTPKZ",	
      "BEGPROG" : "update.BEGPROG",	
      "ENDEPROG" : "update.ENDEPROG",	
      "ABLHINW" : "update.ABLHINW",	
      "V_ZWSTAND" : "update.V_ZWSTAND",	
      "N_ZWSTAND" : "update.N_ZWSTAND",	
      "V_ZWSTNDAB" : "update.V_ZWSTNDAB",	
      "N_ZWSTNDAB" : "update.N_ZWSTNDAB",	
      "V_ZWSTVOR" : "update.V_ZWSTVOR",	
      "N_ZWSTVOR" : "update.N_ZWSTVOR",	
      "V_ZWSTDIFF" : "update.V_ZWSTDIFF",	
      "N_ZWSTDIFF" : "update.N_ZWSTDIFF",	
      "QDPROC" : "update.QDPROC",	
      "MRCONNECT" : "update.MRCONNECT",	
      "op_indicator" : "update.op_indicator",
      "op_timestamp" : "update.op_timestamp",	
      "insertedTS" : "update.insertedTS"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "MANDT"	: "update.MANDT",
      "BELNR"	: "update.BELNR",
      "BELZEILE" : "update.BELZEILE",	
      "EQUNR"	: "update.EQUNR",
      "GERAET" : "update.GERAET",	
      "MATNR"	: "update.MATNR",
      "ZWNUMMER" : "update.ZWNUMMER",
      "INDEXNR" : "update.INDEXNR",
      "ABLESGR" : "update.ABLESGR",	
      "ABLESGRV" : "update.ABLESGRV",	
      "ATIM" : "update.ATIM",	
      "ATIMVA" : "update.ATIMVA",	
      "ADATMAX" : "update.ADATMAX",	
      "ATIMMAX" : "update.ATIMMAX",	
      "THGDATUM" : "update.THGDATUM",	
      "ZUORDDAT" : "update.ZUORDDAT",	
      "REGRELSORT" : "update.REGRELSORT",	
      "ABLBELNR" : "update.ABLBELNR",	
      "LOGIKNR" : "update.LOGIKNR",	
      "LOGIKZW" : "update.LOGIKZW",	
      "ISTABLART" : "update.ISTABLART",	
      "ISTABLARTVA" : "update.ISTABLARTVA",	
      "EXTPKZ" : "update.EXTPKZ",	
      "BEGPROG" : "update.BEGPROG",	
      "ENDEPROG" : "update.ENDEPROG",	
      "ABLHINW" : "update.ABLHINW",	
      "V_ZWSTAND" : "update.V_ZWSTAND",	
      "N_ZWSTAND" : "update.N_ZWSTAND",	
      "V_ZWSTNDAB" : "update.V_ZWSTNDAB",	
      "N_ZWSTNDAB" : "update.N_ZWSTNDAB",	
      "V_ZWSTVOR" : "update.V_ZWSTVOR",	
      "N_ZWSTVOR" : "update.N_ZWSTVOR",	
      "V_ZWSTDIFF" : "update.V_ZWSTDIFF",	
      "N_ZWSTDIFF" : "update.N_ZWSTDIFF",	
      "QDPROC" : "update.QDPROC",	
      "MRCONNECT" : "update.MRCONNECT",	
      "op_indicator" : "update.op_indicator",
      "op_timestamp" : "update.op_timestamp",	
      "insertedTS" : "update.insertedTS"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from Delta.`/FileStore/tables/target/delta2/`