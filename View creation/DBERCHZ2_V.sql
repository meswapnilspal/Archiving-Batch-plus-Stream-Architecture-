-- Databricks notebook source
-- MAGIC %sql
-- MAGIC select 
-- MAGIC MANDT  AS  CLIENT,
-- MAGIC BELNR  AS  BILLING_DOC_NO,
-- MAGIC BELZEILE  AS  LINE_ITEM,
-- MAGIC EQUNR  AS  EQUIPMENT,
-- MAGIC GERAET  AS  DEVICE,
-- MAGIC MATNR  AS  MATERIAL,
-- MAGIC ZWNUMMER  AS  REGISTER,
-- MAGIC INDEXNR  AS  CONSNO_OF_RR,
-- MAGIC ABLESGR  AS  MR_REASON,
-- MAGIC ABLESGRV  AS  PREV_MR_REASON,
-- MAGIC ATIM  AS  MR_TIME,
-- MAGIC ATIMVA  AS  TOD_OF_PREV_MR,
-- MAGIC ADATMAX  AS  DATE_OF_MAX_MR,
-- MAGIC ATIMMAX  AS  TIME_OF_MAX_MR,
-- MAGIC THGDATUM  AS  GAS_ALLOC_DATE,
-- MAGIC ZUORDDAT  AS  MR_ALLOC_DATE,
-- MAGIC REGRELSORT  AS  SORT_REG_REL,
-- MAGIC ABLBELNR  AS  INT_MR_DOC_ID1,
-- MAGIC LOGIKNR  AS  LOG_DEV_NO,
-- MAGIC LOGIKZW  AS  LOGICAL_REG_NO,
-- MAGIC ISTABLART  AS  MR_TYPE,
-- MAGIC ISTABLARTVA  AS  MR_TYPE_PREV_MR,
-- MAGIC EXTPKZ  AS  MR_RES_ORIGIN,
-- MAGIC BEGPROG  AS  START_FORECAST,
-- MAGIC ENDEPROG  AS  END_OF_FORECAST,
-- MAGIC ABLHINW  AS  MR_NOTE,
-- MAGIC QDPROC  AS  QTY_DET_PROCED,
-- MAGIC MRCONNECT  AS  INT_MR_DOC_ID,
-- MAGIC V_ZWSTAND  AS  PREDEC_PLACES_IN_MR,
-- MAGIC N_ZWSTAND  AS  DEC_PLACES_IN_MR,
-- MAGIC V_ZWSTNDAB  AS  PREDEC_BILLED_MR,
-- MAGIC N_ZWSTNDAB  AS  DP_BILLED_MR,
-- MAGIC V_ZWSTVOR  AS  V_ZWSTVOR,
-- MAGIC N_ZWSTVOR  AS  N_ZWSTVOR,
-- MAGIC V_ZWSTDIFF  AS  V_ZWSTDIFF,
-- MAGIC N_ZWSTDIFF  AS  N_ZWSTDIFF
-- MAGIC from DBERCHZ2