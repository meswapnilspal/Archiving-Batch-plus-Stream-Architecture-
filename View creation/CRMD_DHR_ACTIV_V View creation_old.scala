// Databricks notebook source
// MAGIC %md
// MAGIC Databricks :                   
// MAGIC Table - cdspocchange.crm_CRMD_DHR_ACTIV     
// MAGIC View - cdspocchange.CRMD_DHR_ACTIV_V
// MAGIC
// MAGIC SQL :    
// MAGIC Table - SAPCRM.crm_CRMD_DHR_ACTIV     
// MAGIC View - dbo.CRMD_DHR_ACTIV_V

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE VIEW cdspocchange.CRMD_DHR_ACTIV_V AS SELECT
// MAGIC 	 CLIENT                       CLIENT,
// MAGIC 	 GUID                         OBJECT_GUID,
// MAGIC 	 CHANGED_AT                   CHANGED_ON,
// MAGIC 	 OBJECT_ID                    ID_1,
// MAGIC 	 PRIVATE_FLAG                 PRIVATE,
// MAGIC 	 CATEGORY                     CATEGORY,
// MAGIC 	 PERSON_RESP                  EMPLOYEE_RESP,
// MAGIC 	 PRIORITY                     PRIORITY,
// MAGIC 	 DESCRIPTION_UC               DESCRIPTION,
// MAGIC 	 ACTIVITY_PARTNER             ACTIVITYPARTNER,
// MAGIC 	 PROCESS_TYPE                 TRANSACTION_TYPE,
// MAGIC 	 CONTACT_PERSON               CONTACT_PERS,
// MAGIC 	 DIRECTION                    DIRECTION,
// MAGIC 	 FROMDATE                     START_DATE,
// MAGIC 	 TODATE                       END_DATE,
// MAGIC 	 STAT_OPEN                    STAT_OPEN   ,                     
// MAGIC 	 SALES_ORG_RESP               ORG_UNIT_SALES,
// MAGIC 	 STATUS                       STATUS,
// MAGIC 	 USER_STAT_PROC               STATUS_PROFILE,
// MAGIC 	 ACT_TOT_DURA                 DURATION_VALUE,
// MAGIC 	 ACT_TOT_UNIT                 TIME_UNIT,
// MAGIC 	 RESP_AT_PARTNER              RESP_AT_PARTNERS,
// MAGIC 	 CHANNEL_PARTNER              CHANNEL_PARTNER,
// MAGIC 	 CP_PROGRAM                   CH_PARTNER_PROGRAMM,
// MAGIC 	 CP_TYPE                      CHANNEL_PARTNER_TYPE,
// MAGIC 	 CP_STATUS                    CH_PARTNER_STATUS,
// MAGIC 	 PATH_ID                      TERR_HIER_ID,
// MAGIC 	 TERR_ID                      TERRITORY_ID,
// MAGIC 	 SALES_ORG                    SALES_ORG_ID,
// MAGIC 	 SALES_OFFICE                 SALES_OFFICE,
// MAGIC 	 SALES_GROUP                  SALES_GROUP,
// MAGIC 	 CODE_ACT_ST                  CODE,
// MAGIC 	 CODEGRP_ACT_ST               CODE_GROUP,
// MAGIC 	 KATART_ACT_ST                CATALOG,
// MAGIC 	 CATLVLGUID01_IR              CAT_01_S1,
// MAGIC 	 CATLVLGUID02_IR              CAT_02_S1,
// MAGIC 	 CATLVLGUID03_IR              CAT_03_S1,
// MAGIC 	 CATLVLGUID04_IR              CAT_04_S1,
// MAGIC 	 CATLVLGUID05_IR              CAT_05_S1,
// MAGIC 	 CATLVLGUID06_IR              CAT_06_S1,
// MAGIC 	 CATLVLGUID07_IR              CAT_07_S1,
// MAGIC 	 CATLVLGUID08_IR              CAT_08_S1,
// MAGIC 	 CATLVLGUID09_IR              CAT_09_S1,
// MAGIC 	 CATLVLGUID10_IR              CAT_10_S1,
// MAGIC 	 EEW_R_ACT_DUMMY               DUMMY 
// MAGIC FROM cdspocchange.crm_CRMD_DHR_ACTIV

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from cdspocchange.crm_CRMD_DHR_ACTIV
// MAGIC union all
// MAGIC select count(*) from cdspocchange.CRMD_DHR_ACTIV_V

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE VIEW CRMD_DHR_ACTIV_V AS SELECT
// MAGIC 	 CLIENT                       CLIENT,
// MAGIC 	 GUID                         OBJECT_GUID,
// MAGIC 	 CHANGED_AT                   CHANGED_ON,
// MAGIC 	 OBJECT_ID                    ID_1,
// MAGIC 	 PRIVATE_FLAG                 PRIVATE,
// MAGIC 	 CATEGORY                     CATEGORY,
// MAGIC 	 PERSON_RESP                  EMPLOYEE_RESP,
// MAGIC 	 PRIORITY                     PRIORITY,
// MAGIC 	 DESCRIPTION_UC               DESCRIPTION,
// MAGIC 	 ACTIVITY_PARTNER             ACTIVITYPARTNER,
// MAGIC 	 PROCESS_TYPE                 TRANSACTION_TYPE,
// MAGIC 	 CONTACT_PERSON               CONTACT_PERS,
// MAGIC 	 DIRECTION                    DIRECTION,
// MAGIC 	 FROMDATE                     START_DATE,
// MAGIC 	 TODATE                       END_DATE,
// MAGIC 	 STAT_OPEN                    STAT_OPEN,                     
// MAGIC 	 SALES_ORG_RESP               ORG_UNIT_SALES,
// MAGIC 	 STATUS                       STATUS,
// MAGIC 	 USER_STAT_PROC               STATUS_PROFILE,
// MAGIC 	 ACT_TOT_DURA                 DURATION_VALUE,
// MAGIC 	 ACT_TOT_UNIT                 TIME_UNIT,
// MAGIC 	 RESP_AT_PARTNER              RESP_AT_PARTNERS,
// MAGIC 	 CHANNEL_PARTNER              CHANNEL_PARTNER,
// MAGIC 	 CP_PROGRAM                   CH_PARTNER_PROGRAMM,
// MAGIC 	 CP_TYPE                      CHANNEL_PARTNER_TYPE,
// MAGIC 	 CP_STATUS                    CH_PARTNER_STATUS,
// MAGIC 	 PATH_ID                      TERR_HIER_ID,
// MAGIC 	 TERR_ID                      TERRITORY_ID,
// MAGIC 	 SALES_ORG                    SALES_ORG_ID,
// MAGIC 	 SALES_OFFICE                 SALES_OFFICE,
// MAGIC 	 SALES_GROUP                  SALES_GROUP,
// MAGIC 	 CODE_ACT_ST                  CODE,
// MAGIC 	 CODEGRP_ACT_ST               CODE_GROUP,
// MAGIC 	 KATART_ACT_ST                CATALOG,
// MAGIC 	 CATLVLGUID01_IR              CAT_01_S1,
// MAGIC 	 CATLVLGUID02_IR              CAT_02_S1,
// MAGIC 	 CATLVLGUID03_IR              CAT_03_S1,
// MAGIC 	 CATLVLGUID04_IR              CAT_04_S1,
// MAGIC 	 CATLVLGUID05_IR              CAT_05_S1,
// MAGIC 	 CATLVLGUID06_IR              CAT_06_S1,
// MAGIC 	 CATLVLGUID07_IR              CAT_07_S1,
// MAGIC 	 CATLVLGUID08_IR              CAT_08_S1,
// MAGIC 	 CATLVLGUID09_IR              CAT_09_S1,
// MAGIC 	 CATLVLGUID10_IR              CAT_10_S1,
// MAGIC 	 EEW_R_ACT_DUMMY               DUMMY 
// MAGIC FROM  SAPCRM.crm_CRMD_DHR_ACTIV