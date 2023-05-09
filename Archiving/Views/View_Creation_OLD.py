# Databricks notebook source
dberchz1_part1 = spark.sql("select * from cods.qi6_dberchz1")
dberchz1_part2 = spark.sql("select * from cods.qi6_dberchz1part2dberchz1")
dberchz1_part3 = spark.sql("select * from cods.qi6_dberchz1part3dberchz1")
dberchz1_part4 = spark.sql("select * from cods.qi6_dberchz1part4dberchz1")
dberchz1_part5 = spark.sql("select * from cods.qi6_dberchz1part5dberchz1")
dberchz2 = spark.sql("select * from cods.qi6_dberchz2")
dberchz3 = spark.sql("select * from cods.qi6_dberchz3")

# COMMAND ----------

d1_part1 = dberchz1_part1.drop('op_indicator','op_timestamp')
d1_part2 = dberchz1_part2.drop('op_indicator','op_timestamp')
d1_part3 = dberchz1_part3.drop('op_indicator','op_timestamp')
d1_part4 = dberchz1_part4.drop('op_indicator','op_timestamp')
d1_part5 = dberchz1_part5.drop('op_indicator','op_timestamp')

d2 = dberchz2.drop('op_indicator','op_timestamp')
d3 = dberchz3.drop('op_indicator','op_timestamp')

# COMMAND ----------

d1_part5.count()

# COMMAND ----------

d1 = d1_part1.unionAll(d1_part2).unionAll(d1_part3).unionAll(d1_part4).unionAll(d1_part5)

# COMMAND ----------

d1.createGlobalTempView("DBERCHZ1_V")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from global_temp.DBERCHZ1_V

# COMMAND ----------

d2.createGlobalTempView("DBERCHZ2_V")

# COMMAND ----------

d3.createGlobalTempView("DBERCHZ3_V")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table global_temp.DBERCHZ2_V;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table global_temp.DBERCHZ3_V;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted cods.qi6_dberchz3;

# COMMAND ----------

