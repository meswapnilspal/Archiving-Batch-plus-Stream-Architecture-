-- Databricks notebook source
CREATE VIEW IF NOT EXISTS cods.DBERCHZ1_V AS 
Select * from Delta.`abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz1`
UNION ALL
Select * from Delta.`abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz1part2dberchz1`
UNION ALL
Select * from Delta.`abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz1part3dberchz1`
UNION ALL
Select * from Delta.`abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz1part4dberchz1`
UNION ALL
Select * from Delta.`abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz1part5dberchz1` ;

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS cods.DBERCHZ2_V AS 
Select * from Delta.`abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz2` ;

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS cods.DBERCHZ3_V AS 
Select * from Delta.`abfss://raw@dtecuprodedaadl05.dfs.core.windows.net/cods/qi6_dberchz3` ;

-- COMMAND ----------

SELECT COUNT(*) FROM cods.DBERCHZ1_V;

-- COMMAND ----------

