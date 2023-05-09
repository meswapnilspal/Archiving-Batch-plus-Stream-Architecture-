// Databricks notebook source
// MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

// COMMAND ----------

// %sql
// select * from table_metadata_1

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.sqlContext.implicits._
import org.apache.spark.sql.functions.col

// COMMAND ----------

dbutils.widgets.text("Table_name", "", "Source Table Name")
val tbl_nm = dbutils.widgets.get("Table_name")
dbutils.widgets.text("meta_tbl", "", "metadata Table Name")
val metadatatbl = dbutils.widgets.get("meta_tbl")
dbutils.widgets.text("partcol", "", "Partitioned columns seperated by comma")
// val partcols = dbutils.widgets.get("partcol")     //TWAERS, PREISTUF 
val part_file_path = "abfss://dev@dtecuprodedaadl02.dfs.core.windows.net/ISU_test/ISU_TABLE_Partition.csv"

// COMMAND ----------


val part_df1 = spark.read.format("csv").option("Header", true).load(part_file_path)
.toDF("sr_no", "Schema", "table","column_nm","data_type", "sap_key", "desc","key_flag","note", "meeting_note")   
val part_df = part_df1.select("Schema", "table","column_nm","data_type","key_flag", "note")
// .where($"firstName" === "xiangrui")
.where($"key_flag" === "Y" && $"table" === lit(tbl_nm))
part_df.createOrReplaceTempView("part_tbl")

// COMMAND ----------

val part_key = spark.sql(s""" select column_nm from part_tbl """).collect().mkString("").replaceAll("[\\[\\]]","")

val part_format = spark.sql(s""" select data_type from part_tbl """).collect().mkString("").replaceAll("[\\[\\]]","")

// COMMAND ----------

val keydata = spark.sql(s""" select Field_Name, 
case Data_Type when "CLNT" then "string"
when "CHAR" then "string"
when "NUMC" then	"int"
when "DATS" then	"string"
when "UNIT" then	"string"
when "INT4" then	"decimal" 
when "DEC" then	"decimal" 
when "CURR" then	"decimal" 
when "CUKY" then	"string"
end as Scala_data_type
from $metadatatbl where Key_field is not null and Table_Name = "$tbl_nm" 
order by Table_position asc """).collect().mkString(";").replaceAll("[\\[\\]]","").replace(",", "   ").replace(";", " , ").replace("double", "decimal(18,4)")

// COMMAND ----------

val tbl_Schema = spark.sql(s""" select Field_Name, 
case Data_Type when "CLNT" then "string"
when "CHAR" then "string"
when "NUMC" then	"int"
when "DATS" then	"string"
when "UNIT" then	"string"
when "INT4" then	"double" 
when "DEC" then	 "double" 
when "CURR" then	"double" 
when "CUKY" then	"string"
end as Scala_data_type
from $metadatatbl where Table_Name = "$tbl_nm" 
order by Table_position asc """).collect().mkString(";").replaceAll("[\\[\\]]","").replace(",", "   ").replace(";", " , ").replace("double", "decimal(18,4)")

// COMMAND ----------

spark.sql(s""" drop table if exists $tbl_nm """)
if (partcols == "")
{  
  print("no part column \n") 
  spark.sql(s""" create table if not exists $tbl_nm ($tbl_Schema) 
  using delta  """)
}
else
{
    print("part column present \n")
    spark.sql(s""" create table if not exists $tbl_nm ($tbl_Schema) 
    using delta partitioned by ($partcols)  """)
}

// COMMAND ----------

// MAGIC %sql
// MAGIC desc  DBERCHZ3

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from  DBERCHZ3

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from table_metadata_1 --where Table_Name = "DBERCHZ3"