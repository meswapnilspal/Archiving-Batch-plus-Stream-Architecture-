// Databricks notebook source
// MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

dbutils.widgets.text("env", "", "Environment")
val env = dbutils.widgets.get("env")

// COMMAND ----------

//  raw_path = "abfss://"+env+"@"+adlsgen2storageaccountname+".dfs.core.windows.net/Dummy_test/ala_test/raw/dopi/landing/"
val raw_path = "/mnt/dev/raw/piba/test/"
val circuit_pt = raw_path + "scada"
val sfi_pt = raw_path + "sfi"
val transformer_pt = raw_path + "transformer"
val processedFolder = "abfss://"+env+"@"+adlsgen2storageaccountname+".dfs.core.windows.net/raw/dopi/landing_adls/"
val curated_path = "abfss://"+env+"@"+adlsgen2storageaccountname+".dfs.core.windows.net/curated/do/ala/"

// COMMAND ----------

val circuit_schema = StructType(Array(
    StructField("circuit",StringType,true),
    StructField("read_time",StringType,true),
    StructField("current_x", StringType, true),
    StructField("current_y", StringType, true),
    StructField("current_z", StringType, true)
    ))
val substation_schema = StructType(Array(
    StructField("circuit",StringType,true),
    StructField("read_time",StringType,true),
    StructField("current_x", StringType, true),
    StructField("current_y", StringType, true),
    StructField("current_z", StringType, true)
    ))
val sfi_schema = StructType(Array(
    StructField("sfi",StringType,true),
    StructField("read_time",StringType,true),
    StructField("current_x", StringType, true),
    StructField("current_y", StringType, true),
    StructField("current_z", StringType, true)
    ))

// COMMAND ----------

import org.apache.hadoop.fs._
def filecopy (sourcepath : String, targetpath : String)  = {
  val p1 = new Path(sourcepath) //processing_folder)
   val fs = p1.getFileSystem(spark.sessionState.newHadoopConf)
   val ls1 = fs.listStatus(p1).sortBy(_.getModificationTime).filter(_.isFile)
   ls1.foreach( x => {
         val f = x.getPath.toString
         val filenm = x.getPath.getName.toString
         //println(f)
         //println(filenm)
         dbutils.fs.mv(f, targetpath +filenm)
       } )
}

// COMMAND ----------

// filecopy(circuit_pt, processedFolder+ "/circuit/")

// COMMAND ----------

val df_circuit = spark.read.format("csv").option("sep", "\t").option("header", "true").schema(circuit_schema).load(circuit_pt).dropDuplicates()
val dfct = df_circuit.withColumn("yy", year(to_date($"read_time","M/d/yyyy h:mm:ss a" ))).withColumn("mm", month(to_date($"read_time","M/d/yyyy h:mm:ss a" ))).withColumn("dd", dayofmonth(to_date($"read_time","M/d/yyyy h:mm:ss a" )))
dfct.createOrReplaceTempView("tgt_circuit")
val circuit_count = dfct.count()
//    dfct.write.partitionBy("yy", "mm", "dd").format("delta").mode("append").save(curated_path + "circuit/")

val df_sfi = spark.read.format("csv").option("sep", "\t").option("header", "true").schema(sfi_schema).load(sfi_pt).dropDuplicates()
val dfsf = df_sfi.withColumn("yy", year(to_date($"read_time","M/d/yyyy h:mm:ss a" ))).withColumn("mm", month(to_date($"read_time","M/d/yyyy h:mm:ss a" ))).withColumn("dd", dayofmonth(to_date($"read_time","M/d/yyyy h:mm:ss a" )))
dfsf.createOrReplaceTempView("tgt_sfi")
val sfi_count = dfsf.count()
//    dfsf.write.partitionBy("yy", "mm", "dd").format("delta").mode("append").save(curated_path + "sfi/")

val df_transf = spark.read.format("csv").option("sep", "\t").option("header", "true").schema(substation_schema).load(transformer_pt).dropDuplicates()
val dftr = df_transf.withColumn("yy", year(to_date($"read_time","M/d/yyyy h:mm:ss a" ))).withColumn("mm", month(to_date($"read_time","M/d/yyyy h:mm:ss a" ))).withColumn("dd", dayofmonth(to_date($"read_time","M/d/yyyy h:mm:ss a" )))
dftr.createOrReplaceTempView("tgt_transf")
val transf_count = dftr.count()
//    dftr.write.partitionBy("yy", "mm", "dd").format("delta").mode("append").save(curated_path + "transformer/")

// COMMAND ----------

if (circuit_count > 0) {
  print ("Circuit has " +circuit_count+ " records")
   spark.sql("""
      merge into common.circuit t using tgt_circuit s on
      s.circuit    = t.circuit 
      and  s.read_time   =  t.read_time   
      when not matched then insert
      (circuit  
      ,read_time   
      ,current_x
      ,current_y
      ,current_z
      ,yy
      ,mm
      ,dd
      )
      values (
      s.circuit
      ,s.read_time
      ,s.current_x 
      ,s.current_y 
      ,s.current_z 
      ,s.yy
      ,s.mm
      ,s.dd
      )
      """)
filecopy(circuit_pt, processedFolder+ "/scada/")  
} 
else
{
  print ( "\n No new Circuit file found")
}

if (sfi_count > 0) {
  print ("\n SFI file has " +sfi_count+ " records")
  spark.sql("""
      merge into common.sfi t using tgt_sfi s on
      s.sfi    = t.sfi 
      and  s.read_time   =  t.read_time   
      when not matched then insert
      (sfi  
      ,read_time   
      ,current_x
      ,current_y
      ,current_z
      ,yy
      ,mm
      ,dd
      )
      values (
      s.sfi
      ,s.read_time
      ,s.current_x 
      ,s.current_y 
      ,s.current_z 
      ,s.yy
      ,s.mm
      ,s.dd
      )
      """)
  filecopy(sfi_pt, processedFolder+ "/sfi/")  
}  
else
{
  print ( "\n No new SFI file found")
}

if (transf_count > 0) {
  print ("\n transformer has " +transf_count+ " records")
  spark.sql("""
      merge into common.transformer t using tgt_transf s on
      s.circuit    = t.circuit 
      and  s.read_time   =  t.read_time   
      when not matched then insert
      (circuit  
      ,read_time   
      ,current_x
      ,current_y
      ,current_z
      ,yy
      ,mm
      ,dd
      )
      values (
      s.circuit
      ,s.read_time
      ,s.current_x 
      ,s.current_y 
      ,s.current_z 
      ,s.yy
      ,s.mm
      ,s.dd
      )
      """)
  filecopy(transformer_pt, processedFolder+ "/transformer/")
}
else
{
  print ( "\n No new Transformer file found")
}

// COMMAND ----------

// spark.sql(""" refresh table common.circuit """)
// spark.sql(""" refresh table common.sfi """)
// spark.sql(""" refresh table common.transformer """) 