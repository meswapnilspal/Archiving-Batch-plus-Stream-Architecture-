# Databricks notebook source
# MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

# COMMAND ----------

# common.dberchz1
# common.dberchz2
# common.dberchz3


# cods_v2.qi6_part1dberchz1
# cods_v2.qi6_part2dberchz1
# cods_v2.qi6_part3dberchz1
# cods_v2.qi6_part4dberchz1
# cods_v2.qi6_part5dberchz1

# cods_v2.dberchz2

# cods_v2.qi6_part1dberchz3
# cods_v2.qi6_part2dberchz3
# cods_v2.qi6_part3dberchz3
# cods_v2.qi6_part4dberchz3
# cods_v2.qi6_part5dberchz3

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS cods_v2.DBERCHZ1_V AS
# MAGIC Select *,'HISTORICAL' AS EPOCH_FLAG from common.dberchz1
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part1dberchz1
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part2dberchz1
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part3dberchz1
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part4dberchz1
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part5dberchz1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS cods_v2.DBERCHZ2_V AS
# MAGIC Select *,'HISTORICAL' AS EPOCH_FLAG from common.dberchz2
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.dberchz2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS cods_v2.DBERCHZ3_V AS
# MAGIC Select *,'HISTORICAL' AS EPOCH_FLAG from common.dberchz3
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part1dberchz3
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part2dberchz3
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part3dberchz3
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part4dberchz3
# MAGIC UNION ALL
# MAGIC Select *,'CURRENT' AS EPOCH_FLAG from cods_v2.qi6_part5dberchz3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cods_v2.DBERCHZ2_V where EPOCH_FLAG = 'CURRENT'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cods_v2.DBERCHZ1_V where EPOCH_FLAG = 'CURRENT'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cods_v2.qi6_part1dberchz1__ct

# COMMAND ----------

# MAGIC %sql
# MAGIC drop cods_v2.DBERCHZ1_V