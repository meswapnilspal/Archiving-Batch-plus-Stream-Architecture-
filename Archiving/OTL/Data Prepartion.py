# Databricks notebook source
# MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import fnmatch

# COMMAND ----------

# Table Name
dbutils.widgets.text("table_name", "", "table name")
table_name = dbutils.widgets.get("table_name")

# Staging location ( source here )
dbutils.widgets.text("staging_location", "", "staging location")
staging_location = "'" + dbutils.widgets.get("staging_location") + "/" + table_name + "/'"

# Curated location ( destination here )
dbutils.widgets.text("curated_location", "", "curated location")
curated_location = "'" + dbutils.widgets.get("curated_location") + "/" + table_name + "/'"

# Curated location for Error Table
dbutils.widgets.text("curated_location_err", "", "curated location for error table")
curated_location_err = "'" + dbutils.widgets.get("curated_location_err") + "/" + table_name + "/'"

# COMMAND ----------

print(table_name)
print(staging_location)
print(curated_location)
print(curated_location_err)

# COMMAND ----------

s = 'table_df = spark.read.format("csv").option("delimiter", "|").option("header","true").option("inferSchema","true").load(' + staging_location + ')'
print(s)
exec(s)


# COMMAND ----------

display(table_df)

# COMMAND ----------

def revisit_header(df=table_df,table_n=table_name):
  if fnmatch.fnmatch(df.columns[0],table_n + "*") == True:
    for i in df.columns:
      df = df.withColumnRenamed(i, i[len(table_n)+1:])
    print(df.columns)
    return df

# COMMAND ----------

newdf = revisit_header()

# COMMAND ----------

display(newdf)
cols_from_file_source = newdf.columns

# COMMAND ----------

# MAGIC %run ./Load_Metadata

# COMMAND ----------

metadf = load_metadata_df()
display(metadf)

# COMMAND ----------

cols_from_metadata = metadf.select("Field_Name").where(col("Table_Name")==table_name).rdd.map(lambda x : x[0]).collect()
print(cols_from_metadata)

# COMMAND ----------

print(cols_from_file_source)

# COMMAND ----------

try:
  test_list1 = sorted(cols_from_file_source)
  test_list2 = sorted(cols_from_metadata)
  print("Schema Matches !!")
except:
  print("Mismatch in schema" + table_name)
  #dbutils.notebook.exit('stop')

# COMMAND ----------

display(newdf)

# COMMAND ----------

def generate_code_for_casting_columns_based_on_metadata(metadf,table_name,dfname,newdfname):
  z = str("")
  cols = metadf.select("Field_Name","delta_types").where(col("Table_Name")==table_name).rdd.map(lambda x : x[0]).collect()
  col_types = metadf.select("Field_Name","delta_types").where(col("Table_Name")==table_name).rdd.map(lambda x : x[1]).collect()
  for i,j in zip(cols,col_types):
    i = '"' + i + '"'
    j = '"' + j + '"'
    z = z + ('.withColumn(' + i + ',col(' + i + ').cast(' + j + '))')
  return(newdfname + ' = ' + dfname + z)

# COMMAND ----------

get_code = generate_code_for_casting_columns_based_on_metadata(metadf,table_name,'newdf','casted_df')
exec(get_code)

# COMMAND ----------

display(casted_df)

# COMMAND ----------

# duplicate removal

def duplicates_removal(dups_df):
  df = dups_df.drop_duplicates()
  keyfields = metadf.select("Field_Name").where(col("Table_Name")==table_name).where(col("Key_Field") == 'X').rdd.map(lambda x : x[0]).collect()
  orderbycol = metadf.select("Field_Name").where(col("Table_Name")==table_name).where(col("Key_Field").isNull()).rdd.map(lambda x : x[0]).collect()

  windowspec = Window.partitionBy(keyfields).orderBy(orderbycol[0])
  table_archive = df.withColumn("row_number",row_number().over(windowspec)).select("*").where(col("row_number")==1)
  table_erroneous = df.withColumn("row_number",row_number().over(windowspec)).select("*").where(col("row_number")>1)
  return (table_archive,table_erroneous)

# COMMAND ----------

archive_df,erroneous_df = duplicates_removal(casted_df)

# COMMAND ----------

# display(archive_df)

# COMMAND ----------

# def generate_code_for_creating_partition_column_based_on_type(metadf,table_name,dfname,newdfname):  
#   # get partition column from metatable
#   col1 = metadf.select("Field_Name").where(col("Table_Name")==table_name).where(col("Partition_Key") == 'Y').rdd.map(lambda x : x[0]).collect()
#   #print(col1[0])
#   col2 = metadf.select('Partition_Key_Encoding').where(col('Table_Name')==table_name).where(col('Partition_Key') == 'Y').rdd.map(lambda x : x[0]).collect()
#   #print(col2[0])
#   column_for_partition_logic = col1[0]
#   partition_logic_encoding = int(col2[0])
  
#   # decide partitioning logic based on encoding, and create partition column
#   Logic_1 = newdfname + " = " + dfname + ".withColumn('partitioncol',expr('substring(" + column_for_partition_logic + ",length(" + column_for_partition_logic +") - 2,length(" + column_for_partition_logic + "))'))"
#   #print(Logic_1)
#   Logic_2 = newdfname + " = " + dfname + ".withColumn('partitioncol',substring('" + column_for_partition_logic + "',1,6))"
#   #print(Logic_2)

#   if partition_logic_encoding == 1:
#     return(Logic_1)
#   elif partition_logic_encoding == 2:
#     return(Logic_2)
#   else:
#     return("print('no partition')")

# COMMAND ----------

# get_code = generate_code_for_creating_partition_column_based_on_type(metadf,table_name,'archive_df','archive_part_col_added_df')
# print(get_code)
# exec(get_code)

# COMMAND ----------

# get_code = generate_code_for_creating_partition_column_based_on_type(metadf,table_name,'erroneous_df','erroneous_part_col_added_df')
# exec(get_code)

# COMMAND ----------

def generate_code_for_adding_flag_columns(metadf,table_name,dfname,newdfname):
    col1 = metadf.select("Field_Name").where(col("Table_Name")==table_name).where(col("Partition_Key") == 'Y').rdd.map(lambda x : x[0]).collect()
    if col1:
        Logic_1 = newdfname + " = " + dfname + ".withColumn('partitioncol',lit('ARCHIVE')).withColumn('EPOCH_FLAG',lit('ARCHIVE'))"
    else:
        Logic_1 = newdfname + " = " + dfname + ".withColumn('EPOCH_FLAG',lit('ARCHIVE'))"
    return(Logic_1)

# COMMAND ----------

get_code = generate_code_for_adding_flag_columns(metadf,table_name,'archive_df','archive_part_col_added_df')
print(get_code)
exec(get_code)

# COMMAND ----------

get_code = generate_code_for_adding_flag_columns(metadf,table_name,'erroneous_df','erroneous_part_col_added_df')
print(get_code)
exec(get_code)

# COMMAND ----------

display(archive_part_col_added_df)

# COMMAND ----------

display(erroneous_part_col_added_df)

# COMMAND ----------

archive_final_DF = archive_part_col_added_df.withColumn("EDA_Creation_date",current_timestamp())
display(archive_final_DF)

# COMMAND ----------

erroneous_final_DF = erroneous_part_col_added_df.withColumn("EDA_Creation_date",current_timestamp())
display(erroneous_final_DF)

# COMMAND ----------

archive_write_DF = archive_final_DF.drop("row_number")
erroneous_write_DF = erroneous_final_DF.drop("row_number")

# COMMAND ----------

display(archive_write_DF)

# COMMAND ----------

display(erroneous_write_DF)

# COMMAND ----------

# s = 'archive_write_DF.write.format("delta").mode("append").partitionBy("partitioncol").save(' + curated_location + ')'
# exec(s)

# COMMAND ----------

s = 'archive_write_DF.write.format("delta").mode("append").save(' + curated_location + ')'
exec(s)

# COMMAND ----------

s = 'erroneous_write_DF.write.format("delta").mode("append").save(' + curated_location_err + ')'
exec(s)

# COMMAND ----------

