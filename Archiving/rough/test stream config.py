# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.dtecuprodedaadl05.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dtecuprodedaadl05.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dtecuprodedaadl05.dfs.core.windows.net", "ea7dd103-75c8-40b7-af7d-0eb52c60889b")
spark.conf.set("fs.azure.account.oauth2.client.secret.dtecuprodedaadl05.dfs.core.windows.net", "A4rVH~xHrbDv69NgKC7hPvtc.8_x_iMq9p")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dtecuprodedaadl05.dfs.core.windows.net", "https://login.microsoftonline.com/8e61d5fe-7749-4e76-88ee-6d8799ae8143/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cods.qi6_dberchz1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cods.qi6_dberchz1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cods.qi6_dberchz2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cods.qi6_dberchz2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cods.qi6_dberchz3

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cods.qi6_dberchz3

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table cods.qi6_dberchz1

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted cods.qi6_dberchz1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cods.qi6_dberchz1 limit 10

# COMMAND ----------

