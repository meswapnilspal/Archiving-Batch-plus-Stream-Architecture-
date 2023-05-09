# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("table_name", "", "table name")
table_name = dbutils.widgets.get("table_name")

dbutils.widgets.text("metadata_path", "", "metadata path")
metadata_path = dbutils.widgets.get("metadata_path")

dbutils.widgets.text("db_name", "", "db name")
db_name = dbutils.widgets.get("db_name")

dbutils.widgets.text("source_location", "", "table location")
source_location = '"' + dbutils.widgets.get("source_location") + '/' + table_name + '/"'

dbutils.widgets.text("dest_location", "", "destination location")
dest_location = '"' + dbutils.widgets.get("dest_location") + '/' + table_name + '/"'

# COMMAND ----------

#metadata_path = "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/ISU2_metadata_Copy.csv"

metadataDF = spark.read.option("header", "true").csv(metadata_path)

# COMMAND ----------

display(metadataDF)

# COMMAND ----------

with_delta_types_DF = metadataDF.withColumn("delta_types", when(col("Data type") == None,"STRING")
                                  .when(col("Data type") == "CLNT","STRING")
                                  .when(col("Data type") == "CHAR","STRING")
                                  .when(col("Data type") == "NUMC","int")
                                  .when(col("Data type") == "DATS","STRING")
                                  .when(col("Data type") == "UNIT","STRING")
                                  .when(col("Data type") == "INT4","double")
                                  .when(col("Data type") == "DEC","double")
                                  .when(col("Data type") == "CURR","double")
                                  .when(col("Data type") == "CUKY","STRING")
                                  .otherwise("STRING"))

# COMMAND ----------

display(with_delta_types_DF)

# COMMAND ----------

with_reqd_col_DF = with_delta_types_DF.select(col("TABLE"),col("TECH FIELD NAME"),col("delta_types"),col("DESCRIPTION"),col("Partition Logic Encoding")).where(col("TABLE") == table_name )

# COMMAND ----------

def generate_schema(column_name,types_name,comment):
  columns_list = with_reqd_col_DF.select(column_name).rdd.map(lambda x : x[0]).collect()
  types_list = with_reqd_col_DF.select(types_name).rdd.map(lambda x : x[0]).collect()
  comments_list = with_reqd_col_DF.select(comment).rdd.map(lambda x : x[0]).collect()
  col1 ='('
  col2 =''
  for i,j,k in zip(columns_list[:-1],types_list[:-1],comments_list[:-1]):
     col1 = col1 + ' ' + i + ' ' + j + ' ' + 'COMMENT' + ' ' + '"' + ' ' + k + ' ' + '"' + ' ' + ", "
  col2 = columns_list[-1] + ' ' + types_list[-1] + ' ' + 'COMMENT' + ' ' + '"' + ' ' + comments_list[-1] + ' ' + '"'
  col = col1 + ' ' + col2 + ' )'
  return col

# COMMAND ----------

getschema = generate_schema("TECH FIELD NAME","delta_types","DESCRIPTION")

# COMMAND ----------

# spark.sql(f"""
# CREATE TABLE {db_name}.{table_name} {getschema}
# USING DELTA
# PARTITIONED BY (OPBEL)
# LOCATION {table_location} """)

spark.sql(f"""
CREATE TABLE {db_name}.{table_name} {getschema}
USING DELTA
LOCATION {table_location} """)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted {db_name}.{table_name}

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table {db_name}.{table_name}

# COMMAND ----------

# %sql
# drop table {db_name}.{table_name};

# COMMAND ----------

