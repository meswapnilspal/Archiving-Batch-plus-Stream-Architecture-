# Databricks notebook source
# MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("table_name", "", "table name")
table_name = dbutils.widgets.get("table_name")

dbutils.widgets.text("db_name", "", "db name")
db_name = dbutils.widgets.get("db_name")

dbutils.widgets.text("curated_location", "", "curated location")
curated_location = "'" + dbutils.widgets.get("curated_location") + "/" + table_name + "/'"

# Curated location for Error Table
dbutils.widgets.text("curated_location_err", "", "curated location for error table")
curated_location_err = "'" + dbutils.widgets.get("curated_location_err") + "/" + table_name + "/'"

# COMMAND ----------

# MAGIC %run "./Load_Metadata"

# COMMAND ----------

metadf = load_metadata_df()

# COMMAND ----------

with_reqd_col_DF = metadf.select("*").where(col("Table_Name") == table_name )

# COMMAND ----------

def generate_schema(column_name,types_name,comment,M_DF):
    columns_list = M_DF.select(column_name).rdd.map(lambda x : x[0]).collect()
    types_list = M_DF.select(types_name).rdd.map(lambda x : x[0]).collect()
    comments_list = M_DF.select(comment).rdd.map(lambda x : x[0]).collect()
    col1 ="("
    col2 =""
    for i,j,k in zip(columns_list,types_list,comments_list):
        col1 = col1 + " " + i + " " + j + " " + "COMMENT" + " " + "'" + " " + k + " " + "'" + " " + ', '
    partition_check = M_DF.select("Field_Name").where(col("Partition_Key") == 'Y').rdd.map(lambda x : x[0]).collect()
    if partition_check:
        return col1 + "partitioncol STRING, EPOCH_FLAG STRING, EDA_Creation_date TIMESTAMP ) "
    else:
        return col1 + "EPOCH_FLAG STRING, EDA_Creation_date TIMESTAMP ) "

# COMMAND ----------

getschema = generate_schema("Field_Name","delta_types","Description",with_reqd_col_DF)
print(getschema)

# COMMAND ----------

main_table = 'ISU_' + table_name + '_archive'
erroneous_table = 'ISU_' + table_name + '_arc_err'

# print(main_table)
# print(erroneous_table)
spark.sql(f""" CREATE TABLE IF NOT EXISTS {db_name}.{main_table} {getschema} USING DELTA LOCATION {curated_location} """)
spark.sql(f""" CREATE TABLE IF NOT EXISTS {db_name}.{erroneous_table} {getschema} USING DELTA LOCATION {curated_location_err} """)

# COMMAND ----------

