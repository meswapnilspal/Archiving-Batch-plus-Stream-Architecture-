-- Databricks notebook source
CREATE VIEW ZV_MOVE_IN ( CLIENT,
       MOVE_IN_DOC,
       CONTRACT_ACCT,
       BUSINESSPARTNER,
       CREATED_ON,
       CREATED_BY,
       CONTRACT,
       INSTALLATION,
       PREMISE,
       MOVE_IN_DATE,
       BB_PLAN_CRTD,
       DATA_TRNSF_CONT,
       CONNECTION_OBJ,
       ACTUAL_MI_DATE,
       BPC_CCLASS,
       BPC_ACTIVITY,
       FORM,
       AUTOMATIC_WL,
       PRINT_REQUEST,
       CHANGED_ON,
       CHANGED_BY,
       REVERSED,
       WANT_DATE ) AS SELECT
       T1.MANDT,
       -- client
T1.EINZBELEG,
       --MI Doc
T1.VKONT,
       -- CA
T1.KUNDE,
       -- BP
T1.ERDAT,
       -- Crtd On
T1.ERNAM,
       -- By
T2.VERTRAG,
       --contract
T2.ANLAGE,
       -- Install
T2.VSTELLE,
       -- Premise
T2.EINZDAT,
       --MI Dt
T2.ABSAUT,
       -- BBP 
 T2.XVERA,
       -- Trnf
T3.HAUS,
       -- Con obj.
T1.BEZUGSDAT,
       -- act MI Dt
T1.BPC_CCLASS,
       -- Class
T1.BPC_ACTIVITY,
       -- Act
T1.FORMBEGR,
       -- From
T1.AUTBEGR,
       -- Auto
T1.DELAYED_PRINT,
       -- Print
T1.AEDAT,
       -- Changed On
T1.AENAM,
       -- By
T1.STORNOKZ,
       -- Reversed
T1.ZZWANTDATE -- Want Date

FROM PI6.EEIN T1,
       PI6.EEINV T2,
       PI6.EVBS T3 
WHERE T1.MANDT = T2.MANDT 
AND T1.EINZBELEG = T2.EINZBELEG 
AND T2.MANDT = T3.MANDT 
AND T2.VSTELLE = T3.VSTELLE