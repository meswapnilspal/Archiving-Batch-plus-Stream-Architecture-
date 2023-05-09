# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

tablelocation = ''

# COMMAND ----------

#
fulldata_loc = 'abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/'
fulldata_tables = ['qi6_dberchz1','qi6_dberchz2','qi6_dberchz3']
cdc_location = ''

# COMMAND ----------


cdc_data = spark.read.format('delta') \
  .option('inferSchema', 'True') \
  .option('header', 'True') \
  .load(cdc_location)

# COMMAND ----------

deletes_to_process = cdc_data.select('*').where(col('header__operation') == 'DELETE')
upserts_to_process = cdc_data.select('*').where(col('header__operation') == 'UPDATE' & col('header__operation') == 'INSERT' ) # YET TO BE TESTED



# COMMAND ----------

#Step 4 : Merge logic 

from delta.tables import *

deltaTableTarget = DeltaTable.forPath(spark, tablelocation)
deltaTableUpdates = dfupdate

dfUpdates = upserts_to_process

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