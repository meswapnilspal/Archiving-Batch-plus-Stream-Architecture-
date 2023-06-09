-- Databricks notebook source
-- MAGIC %run "/EDA/Data Engineer/Framework/Secrets-Databricks-Cache"

-- COMMAND ----------


CREATE VIEW "PI6"."ZV_MOVE_OUT" ( "MANDT",
       "MOVE_OUT_DOC",
       "BUSINESSPARTNER",
       "CONTRACT_ACCT",
       "CREATED_ON",
       "CREATED_BY",
       "CONTRACT",
       "INSTALLATION",
       "PREMISE",
       "MOVE_OUT_DATE",
       "CONNECTION_OBJ",
       "ACTUAL_MO_DATE",
       "AUTO_FINAL_BILL",
       "PSTNG_DATE",
       "FORM",
       "PRINT_REQUEST",
       "BPC_CCLASS",
       "BPC_ACTIVITY",
       "EFFECTIVE_DT",
       "REVERSED",
       "CHANGED_ON",
       "CHANGED_BY",
       "WANT_DATE" ) AS SELECT
       T1."MANDT",
       T1."AUSZBELEG",
       T1."KUNDE",
       T1."VKONT",
       T1."ERDAT",
       T1."ERNAM",
       T2."VERTRAG",
       T2."ANLAGE",
       T2."VSTELLE",
       T2."AUSZDAT",
       T3."HAUS",
       T1."DEPARTUREDATE",
       T1."SCHLUSSRECH",
       T1."BUDAT",
       T1."FORMBEST",
       T1."DELAYED_PRINT",
       T1."BPC_CCLASS",
       T1."BPC_ACTIVITY",
       T1."GPVKCHDATE",
       T1."STORAUSZ",
       T1."AEDAT",
       T1."AENAM",
       T1."ZZWANTDATE" 
FROM "PI6"."EAUS" T1,
       "PI6"."EAUSV" T2,
       "PI6"."EVBS" T3 
WHERE T1."MANDT" = T2."MANDT" 
AND T1."AUSZBELEG" = T2."AUSZBELEG" 
AND T2."MANDT" = T3."MANDT" 
AND T2."VSTELLE" = T3."VSTELLE" WITH READ ONLY

-- COMMAND ----------

CREATE VIEW "PI6"."ZV_MOVE_IN" ( "CLIENT",
       "MOVE_IN_DOC",
       "CONTRACT_ACCT",
       "BUSINESSPARTNER",
       "CREATED_ON",
       "CREATED_BY",
       "CONTRACT",
       "INSTALLATION",
       "PREMISE",
       "MOVE_IN_DATE",
       "BB_PLAN_CRTD",
       "DATA_TRNSF_CONT",
       "CONNECTION_OBJ",
       "ACTUAL_MI_DATE",
       "BPC_CCLASS",
       "BPC_ACTIVITY",
       "FORM",
       "AUTOMATIC_WL",
       "PRINT_REQUEST",
       "CHANGED_ON",
       "CHANGED_BY",
       "REVERSED",
       "WANT_DATE" ) AS SELECT
       T1."MANDT",
       -- client
T1."EINZBELEG",
       --MI Doc
T1."VKONT",
       -- CA
T1."KUNDE",
       -- BP
T1."ERDAT",
       -- Crtd On
T1."ERNAM",
       -- By
T2."VERTRAG",
       --contract
T2."ANLAGE",
       -- Install
T2."VSTELLE",
       -- Premise
T2."EINZDAT",
       --MI Dt
T2."ABSAUT",
       -- BBP 
 T2."XVERA",
       -- Trnf
T3."HAUS",
       -- Con obj.
T1."BEZUGSDAT",
       -- act MI Dt
T1."BPC_CCLASS",
       -- Class
T1."BPC_ACTIVITY",
       -- Act
T1."FORMBEGR",
       -- From
T1."AUTBEGR",
       -- Auto
T1."DELAYED_PRINT",
       -- Print
T1."AEDAT",
       -- Changed On
T1."AENAM",
       -- By
T1."STORNOKZ",
       -- Reversed
T1."ZZWANTDATE" -- Want Date

FROM "PI6"."EEIN" T1,
       "PI6"."EEINV" T2,
       "PI6"."EVBS" T3 
WHERE T1."MANDT" = T2."MANDT" 
AND T1."EINZBELEG" = T2."EINZBELEG" 
AND T2."MANDT" = T3."MANDT" 
AND T2."VSTELLE" = T3."VSTELLE" WITH READ ONLY;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW AUFK_JOIN AS SELECT 
-- MAGIC AUFK_V.ORDER, ORDER_TYPE,ORDER_CATEGORY, ENTERED_BY, ENTERED_BY, CREATED_ON, CHANGED_BY, CHANGED_ON, DESCRIPTION, PLANT, BUSSINESS_AREA, CO_AREA, COST_COLLECTOR, LOCATION, LOCATION_PLANT, STATUS, CREATED, RELEASED, COMPLETED, CLOSED, RELEASE, TECH_COMPLETION, CLOSE, DELETION_FLAG, TIME_CREATED, CHANGED_AT, BUSSINESSPARTNER, CONTRACT_ACCT, CONTRACT, INSTALATION, PREMISE, ZZTRANSFRM, MOVE_IN_DOC, MOVE_OUT_DOC, MAT_CODE_1, JOB_COED, EXECUTION_CODE, FEE_AMT, ASSIGNED_USER, ATHOME_FLG, SO_REQUIRED_BY, REQUESTED_PHONE_NUMB, VENDOR, COMM_PREFERENCE, COMMUNICATION, ZZVENDOR_PHONE, MAIN_WORK_CTR, ORD_MIMO_DOC, ORD_CREATED_ON, ORD_CHANGED_ON, ORDER_STATUS, ORD_ACTIVITY_DT  FROM AUFK_V
-- MAGIC INNER JOIN MIMO_LATEST_ORD
-- MAGIC ON AUFK_V.ORDER = IMO_LATEST_ORD.ORDER

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW New_MOVE_OUTS AS SELECT 
MOVE_OUTS.*, AUFK_JOIN.* from MOVE_OUTS LEFT OUTER JOIN AUFK_JOIN 
ON MOVE_OUTS.MOVE_OUT_DOC = AUFK_JOIN.ORD_MIMO_DOC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW New_MOVE_INS AS SELECT 
MOVE_INS.*, AUFK_JOIN.* from MOVE_INS LEFT OUTER JOIN AUFK_JOIN 
ON MOVE_INS.MOVE_IN_DOC = AUFK_JOIN.ORD_MIMO_DOC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW CONTRACT AS SELECT
CONTRACT, COMPANY_CODE, DIVISION, ACT_DETERM_ID,DELETE, PYMT_PLAN_TYPE, STARTING_MONTH, INSTALLATION, CONTRACT_ACCT, MOVE_IN_DATE, MOVE_OUT_DATE, ZZPROC_RELEASE 
,CRM_MOVES_PROC_TXT.TEXT
FROM EVER_V  LEFT OUTER JOIN CRM_MOVES_PROC_TXT ON CRM_MOVES_PROC_TXT.PROCESS = EVER_V.ZZPROC_RELEASE

-- COMMAND ----------

create or replace temporary view MIMO_ORD as 
select New_MOVE_OUTS.*, New_MOVE_INS.* from New_MOVE_OUTS union New_MOVE_INS

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW JOIN1 AS SELECT
M.CONTRACT_ACCT,
M.BUSSINESSPARTNER
M.CONTRACT,
M.INSTALLATION,
M.CONNECTION_OBJ,
M.PREMISE,
M.MIMO_DOC,
M.MIMO_TYPE,
M.MIMO_EFF_DT,
M.MIMO_CRTD_YEAR,
M.MIMO_CRTD_ON,
M.MIMO_CHNG_ON,
M.MIMO_CTRD_BY,
M.MIMO_CHANGE_BY,
M.MIMO_RETRO,
M.MOVE_OUT_DOC,
M.MOVE_OUT_DATE,
M.ACTUAL_MO_DATE,
M.PSTNG_DATE,
M.WANT_DATE,
M.MOVE_IN_DOC,
M.CREATED_ON,
M.CREATED_BY,
M.MOVE_IN_DATE,
M.BB_PLAN_CRTD,
M.DATA_TRNSF_CONT,
M.ACTUAL_MI_DATE,
M.BPC_CCLASS,
M.BPC_ACTIVITY,
M.FORM,
M.PRINT_REQUEST,
M.CHANGED_ON,
M.CHANGED_BY,
M.REVERSED,
M.ORDER,
M.ORDER_TYPE,
M.ORDER_CATEGORY,
M.ORD_CRTD_ON,
M.ORD_CRTD_TIM,
M.ORD_CRTD_BY,
M.ORD_CHNG_ON,
M.ORD_CHNG_TIM,
M.ORD_CHNG_BY,
M.ORD_DESCRIPTION,
M.PLANT,
M.STATUS,
M.CREATED,
M.RELEASED,
M.COMPLETED,
M.CLOSED,
M.RELEASE,
M.TECH_COMPLETION,
M.CLOSE,
M.DELETION_FLAG,
M.ORD_PARTNER,
M.ORD_CONT_ACC,
M.ORD_CONTRACT,
M.ORD_INSTALLA,
M.ORD_PREMISE,
M.ORD_MIMO_DOC,
M.ORD_CREATED_ON,
M.ORD_CHANGED_ON,
M.MAT_CODE_1,
M.JOB_CODE,
M.EXECUTION_CODE,
M.FEE_AMT,
M.ASSIGNED_USER,
M.ATHOME_FLG,
M.SO_REQUESTED_BY,
M.REQUESTED_PHONE_NUMBER,
M.VENDOR,
M.COMM_PREFERENCE,
M.COMMUNICATION,
M.ZZVENDOR_PHONE,
M.MAIN_WORKCTR,
M.ORDER_STATUS,
M.ORD_CRTD_TIM,
M.ORD_ACTIVITY_DT,
C.COMPANY_CODE,
C.DIVISION,
C.ACT_DETERM_ID,
C.DELETE1,
C.INSTALLATION,
C.CONTRACT_ACCT,
C.MOVE_IN_DATE,
C.MOVE_OUT_DATE,
C.ZZPROC_REASON,
C.TEXT
FROM MIMO_ORD M LEFT JOIN CONTRACT C
ON M.CONTRACT = C.CONTRACT

-- COMMAND ----------

-- DBTITLE 1,Aggregation
select count(distinct BUSSINESSPARTNER) COUNT_BP, count(distinct ORDER)  COUNT_ORD from JOIN1