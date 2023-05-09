# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

def load_metadata_df(metadata_path="abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/metadata/Final_metadata.csv"):
  metadataDF = spark.read.option("header", "true").csv(metadata_path)
  with_delta_types_DF = metadataDF.withColumn("delta_types", when(col("Data_Type") == None,"STRING")
                                    .when(col("Data_Type") == "CLNT","STRING")
                                    .when(col("Data_Type") == "CHAR","STRING")
                                    .when(col("Data_Type") == "NUMC","int")
                                    .when(col("Data_Type") == "DATS","STRING")
                                    .when(col("Data_Type") == "UNIT","STRING")
                                    .when(col("Data_Type") == "INT4","double")
                                    .when(col("Data_Type") == "DEC","double")
                                    .when(col("Data_Type") == "CURR","double")
                                    .when(col("Data_Type") == "CUKY","STRING")
                                    .otherwise("STRING"))
  return with_delta_types_DF

# COMMAND ----------

print("Your Metadata dataframe is loaded")
print("Use following syntax to grab it")
print(" <new dataframe> = load_metadata_df(<path of metadata csv>) ")
print("\n")
print("NOTE : csv path parameter is optional as Functions uses default path")

# COMMAND ----------

