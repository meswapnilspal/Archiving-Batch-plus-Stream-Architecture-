{"version":"NotebookV1","origId":1032317636649204,"name":"Execute View Scripts","language":"sql","commands":[{"version":"CommandV1","origId":1032317636649205,"guid":"a03e548d-1a87-410d-9706-709a89a6e6d9","subtype":"command","commandType":"auto","position":1.0,"command":"CREATE\n\tOR REPLACE VIEW EDW_NONPROD_CODS.DFKKCOLL AS\n\nSELECT MANDT AS CLIENT\n\t,OPBEL AS PRINT_DOCUMENT\n\t,INKPS AS COLLECTION_ITEM\n\t,INKGP AS COLLECTION_AGENCY\n\t,BUKRS AS COMPANY_CODE\n\t,GPART AS BUSINESS_PARTN\n\t,VKONT AS CONTRACT_ACCT\n\t,AGDAT AS SUBMISSION_DATE\n\t,AGGRD AS SUBMISSION_REASON\n\t,WAERS AS CURRENCY\n\t,BETRW AS AMOUNT\n\t,BETRZ AS PAYMENT_AMOUNT\n\t,NINKB AS UNCOLLECTABLE\n\t,INTBT AS INTEREST_RECEIVABLE\n\t,CHARB AS CHARGE_RECEIVABLE\n\t,XBLNR AS REFERENCE\n\t,AGSTA AS SUBMISSION_STATUS\n\t,XSOLD AS SOLD\n\t,NRZAS AS PYT_FORM_NO\n\t,RUDAT AS CALLBACK_DATE\n\t,RUGRD AS CALLBACK_REASON\n\t,COLLCASE_ID AS EXTERNAL_CASE_NO\n\t,EPOCH_FLAG\n\t,EDA_CREATION_DATE\nFROM CODS_NONPROD.DFKKCOLL;","commandVersion":1,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663934130781,"submitTime":1663934130735,"finishTime":1663934131448,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"b818debe-28c6-466b-b891-6748ce5155a4"},{"version":"CommandV1","origId":1032317636649206,"guid":"acf8a9ec-ebd2-4c49-a24d-963796fd8bf5","subtype":"command","commandType":"auto","position":2.0,"command":"CREATE\n\tOR REPLACE VIEW EDW_NONPROD_CODS.DFKKCOLLH AS\n\nSELECT MANDT AS CLIENT\n\t,OPBEL AS PRINT_DOCUMENT\n\t,INKPS AS COLLECTION_ITEM\n\t,LFDNR AS SEQUENCE_NUMBER\n\t,INKGP AS COLLECTION_AGENCY\n\t,BUKRS AS COMPANY_CODE\n\t,GPART AS BUSINESS_PARTN\n\t,VKONT AS CONTRACT_ACCT\n\t,AGDAT AS SUBMISSION_DATE\n\t,AGGRD AS SUBMISSION_REASON\n\t,WAERS AS CURRENCY\n\t,BETRW AS AMOUNT\n\t,BETRZ AS PAYMENT_AMOUNT\n\t,NINKB AS UNCOLLECTABLE\n\t,INTBT AS INTEREST_RECEIVABLE\n\t,CHARB AS CHARGE_RECEIVABLE\n\t,XBLNR AS REFERENCE\n\t,AGSTA AS SUBMISSION_STATUS\n\t,AGSTA_OR AS SUBMISSION_ORIG_STATUS\n\t,XSOLD AS SOLD\n\t,NRZAS AS PYT_FORM_NO\n\t,RUDAT AS CALLBACK_DATE\n\t,RUGRD AS CALLBACK_REASON\n\t,AUGBL AS CLEARING_DOC\n\t,STORB AS REVERSED_DOCUMENT\n\t,AENAM AS CHANGED_BY\n\t,ACPDT AS CHANGED_ON\n\t,ACPTM AS CHANGED_TIME\n\t,EPOCH_FLAG\n\t,EDA_CREATION_DATE\nFROM CODS_NONPROD.DFKKCOLLH;","commandVersion":1,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663934161360,"submitTime":1663934161330,"finishTime":1663934161911,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"b6f9a648-bbe9-4ee7-bff5-aa6393625331"},{"version":"CommandV1","origId":1032317636649207,"guid":"70809d00-b312-4ad9-894b-549d6f4a6e10","subtype":"command","commandType":"auto","position":3.0,"command":"CREATE\n\tOR REPLACE VIEW EDW_NONPROD_CODS.DFKKLOCKS AS\n\nSELECT \n\tCLIENT AS CLIENT\n,\tLOOBJ1\tas \tLOCK_OBJECT\n,\tLOTYP\tas \tLOCK_CATEGORY\n,\tPROID\tas \tPROCESS\n,\tLOCKR\tas \tLOCK_REASON\n,\tFDATE\tas \tFROM1\n,\tTDATE\tas \tTO\n,\tGPART\tas \tBUSINESS_PARTN\n,\tVKONT\tas \tCONTRACT_ACCT\n,\tCOND_LOOBJ\tas \tLOCK_OBJECT1\n,\tACTKEY\tas \tACTIVITY\n,\tUNAME\tas \tUSER_NAME\n,\tADATUM\tas \tDATE1\n,\tAZEIT\tas \tTIME1\n,\tPROTECTED\tas \tPROTECTED\n,\tLAUFD\tas \tDATE_ID\n,\tLAUFI\tas \tIDENTIFICATION\n,\tEPOCH_FLAG\n,\tEDA_CREATION_DATE\nFROM CODS_NONPROD.DFKKLOCKS;","commandVersion":1,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663934203036,"submitTime":1663934203001,"finishTime":1663934203500,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"8db1a59e-6f9b-475e-90cd-c2cc2e20bf2e"},{"version":"CommandV1","origId":1032317636649208,"guid":"83df2fb0-6b29-4831-9011-da73cbfd6942","subtype":"command","commandType":"auto","position":4.0,"command":"CREATE\n\tOR REPLACE VIEW EDW_NONPROD_CODS.DFKKLOCKSH AS\n\nSELECT CLIENT AS CLIENT\n\t,LOOBJ1 AS LOCK_OBJECT\n\t,LOTYP AS LOCK_CATEGORY\n\t,PROID AS PROCESS\n\t,LFDNR AS SEQUENCE_NUMBER\n\t,LOCKR AS LOCK_REASON\n\t,FDATE AS FROM1\n\t,TDATE AS TO\n\t,GPART AS BUSINESS_PARTN\n\t,COND_LOOBJ AS LOCK_OBJECT1\n\t,ACTKEY AS ACTIVITY\n\t,UNAME AS USER_NAME\n\t,ADATUM AS DATE1\n\t,AZEIT AS TIME1\n\t,LUNAME AS LAST_USER_NAME\n\t,LDATUM AS DATE_CHANGE\n\t,LZEIT AS TIME\n\t,ZZLOCK_AMT AS LOCK_AMOUNT\n\t,EPOCH_FLAG\n\t,EDA_CREATION_DATE\nFROM CODS_NONPROD.DFKKLOCKSH;","commandVersion":3,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663934431749,"submitTime":1663934431701,"finishTime":1663934432134,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"9c8e54b3-834a-49f1-9980-57fac8264901"},{"version":"CommandV1","origId":1032317636649209,"guid":"31c9727b-9c20-4a1e-98ea-f261b5262677","subtype":"command","commandType":"auto","position":5.0,"command":"CREATE\n\tOR REPLACE VIEW EDW_NONPROD_CODS.DFKKOPCOLL AS\n\nSELECT MANDT AS CLIENT\n\t,LAUFI AS IDENTIFICATION\n\t,LAUFD AS DATE_ID\n\t,INKGP AS COLLECTION_AGENCY\n\t,INKPS AS COLLECTION_ITEM\n\t,OPBEL AS PRINT_DOCUMENT\n\t,OPUPW AS REPETITION_ITEM\n\t,OPUPK AS ITEM\n\t,OPUPZ AS SUB_ITEM\n\t,XSIMU AS SIMULATION_RUN\n\t,BUKRS AS COMPANY_CODE\n\t,GSBER AS BUSINESS_AREA\n\t,GPART AS BUSINESS_PARTN\n\t,VTREF AS CONTRACT\n\t,VKONT AS CONTRACT_ACCT\n\t,HVORG AS MAIN_TRANS\n\t,TVORG AS SUBTRANSACTION\n\t,STAKZ AS STATISTICAL_KEY\n\t,BLDAT AS DOCUMENT_DATE\n\t,BUDAT AS POSTING_DATE\n\t,WAERS AS CURRENCY\n\t,HWAER AS LOCAL_CURRENCY\n\t,FAEDN AS NET_DUE_DATE\n\t,BETRH AS AMOUNT_IN_LC\n\t,BETRW AS AMOUNT\n\t,AUGRD AS CLEARING_REASON\n\t,AGDAT AS SUBMISSION_DATE\n\t,NRZAS AS PYT_FORM_NO\n\t,EPOCH_FLAG\n\t,EDA_CREATION_DATE\nFROM CODS_NONPROD.DFKKOPCOLL;","commandVersion":7,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663934861568,"submitTime":1663934861528,"finishTime":1663934862177,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"c5644006-0b9b-4d8f-b42a-8b113478b8e2"},{"version":"CommandV1","origId":1032317636649210,"guid":"4e2389b0-014f-4f24-86c0-b7ba42f4f25e","subtype":"command","commandType":"auto","position":6.0,"command":"CREATE\n\tOR REPLACE VIEW EDW_NONPROD_CODS.DFKKOPW AS\n\nSELECT MANDT AS CLIENT\n\t,OPBEL AS PRINT_DOCUMENT\n\t,WHGRP AS REPETITION_GRP\n\t,OPUPW AS REPETITION_ITEM\n\t,WHANZ AS NO_OF_ITEMS\n\t,GPART AS BUSINESS_PARTN\n\t,VKONT AS CONTRACT_ACCT\n\t,ABWBL AS SUBSTITUTE_DOC\n\t,ABWTP AS DOC_CAT\n\t,BUDAT AS POSTING_DATE\n\t,FAEDN AS NET_DUE_DATE\n\t,FAEDS AS DISCT_DUE_DATE\n\t,PERSL AS PERIOD_KEY\n\t,XAESP AS CHANGE_LOCK\n\t,MANSP AS DUNNLOCKREASON\n\t,AUGDT AS CLEARING\n\t,AUGBL AS CLEARING_DOC\n\t,AUGBD AS CLRG_POST_DATE\n\t,AUGRD AS CLEARING_REASON\n\t,AUGVD AS CLEAR_VAL_DATE\n\t,AUGOB AS CANCELED\n\t,XAUFL AS BROKEN_DOWN\n\t,XRAGL AS REVERSE_CLRG\n\t,XPYOR AS PAYMENT_ORDER\n\t,STZAL AS PAYMENT_STATUS\n\t,PNNUM AS PRE_NOTIF\n\t,PNHKF AS ORIGIN\n\t,PNEXD AS EXEC_DATE\n\t,DEAKTIV AS DEACTIVATED\n\t,SOLLDAT AS BBR_OR_PART_BILL\n\t,EINMALANF AS ONE_TIME_REQ\n\t,VORAUSZAHL AS ADVANCE_PAYMENT\n\t,ABRABS AS BB_OR_BILL_DATES\n\t,LOGNO AS LOGICAL_LINE_NO\n\t,EPOCH_FLAG\n\t,EDA_CREATION_DATE\nFROM CODS_NONPROD.DFKKOPW;","commandVersion":1,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935037700,"submitTime":1663935037651,"finishTime":1663935038349,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"d10089f3-0700-4e83-8020-4f2cf15a25cd"},{"version":"CommandV1","origId":1032317636649211,"guid":"fe8a3eff-5314-45d5-a69b-a6805c9fc7f2","subtype":"command","commandType":"auto","position":7.0,"command":"CREATE\n\tOR REPLACE VIEW EDW_NONPROD_CODS.FKKMACTIVITIES AS\n\nSELECT MANDT AS CLIENT\n\t,LAUFD AS DATE_ID\n\t,LAUFI AS IDENTIFICATION\n\t,GPART AS BUSINESS_PARTN\n\t,VKONT AS CONTRACT_ACCT\n\t,MAZAE AS DUNNING_COUNTER\n\t,AKZAE AS ACTIVITIES_COUNTER\n\t,ACKEY AS ACTIVITY\n\t,CENTER AS COLLECTIONS_DEPT\n\t,UNIT AS COLLECTION_UNIT\n\t,AGENT AS COLLLECTION_SPECIALIST\n\t,STATUS AS STATUS\n\t,EPOCH_FLAG\n\t,EDA_CREATION_DATE\nFROM CODS_NONPROD.FKKMACTIVITIES;","commandVersion":1,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935066391,"submitTime":1663935066364,"finishTime":1663935066751,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"92b7031b-21b9-48a8-8d32-47c32a9f812a"},{"version":"CommandV1","origId":1032317636649212,"guid":"63526ae3-df36-4bcb-b669-b50a8c5f8605","subtype":"command","commandType":"auto","position":8.0,"command":"CREATE\n\tOR REPLACE VIEW EDW_NONPROD_CODS.FKKMAKO AS\n\nSELECT MANDT AS CLIENT\n\t,LAUFD AS DATE_ID\n\t,LAUFI AS IDENTIFICATION\n\t,GPART AS BUSINESS_PARTN\n\t,VKONT AS CONTRACT_ACCT\n\t,MAZAE AS DUNNING_COUNTER\n\t,AUSDT AS DATE_OF_ISSUE\n\t,MDRKD AS PRINT_DATE\n\t,MAHNV AS DUNNING_PROC\n\t,MGRUP AS GROUPING\n\t,VKONTGRP AS ACCOUNT_GROUP\n\t,VTREFGRP AS CONTRACT_GROUP\n\t,ITEMGRP AS ITEM_GROUP\n\t,ITEMGRP_LAST AS ITEM_GROUP1\n\t,GRPFIELD AS GROUPING_FIELD\n\t,GRPFIELD_LAST AS GROUPING_FIELD1\n\t,STRAT AS COLL_STRATEGY\n\t,STEP AS COLLECTION_STEP\n\t,STEP_LAST AS COLLSTEPLASTDUN\n\t,STEP_REPLACED AS REPLACEDCOLLSTP\n\t,STRAT_CHAMP AS CHAMP_STRATEGY\n\t,TESTS AS TEST_SERIES\n\t,LIMITED AS RESTRICTED_CAP\n\t,RELEA AS FOR_RELEASE\n\t,RELDATE AS RELEASE_BY\n\t,RELGROUP AS RELEASE_GROUP\n\t,NEXDT AS NEXT_DUNNING\n\t,OPBUK AS COMPANY_CODE_GP\n\t,STDBK AS COMPANY_CODE\n\t,GSBER AS BUSINESS_AREA\n\t,SPART AS DIVISION\n\t,VTREF AS CONTRACT\n\t,SUBAP AS SUBAPPLICATION\n\t,VKNT1 AS LEADING_ACCOUNT\n\t,ABWMA AS ALT_DUN_RECIP\n\t,MAHNS AS DUNNING_LEVEL\n\t,MSTYP AS DUNN_LEVEL_CAT\n\t,WAERS AS CURRENCY\n\t,MSALM AS DUNNING_BALANCE\n\t,MSALH AS UNUSED\n\t,RSALM AS DUNN_REDUCTNS\n\t,CHGID AS CHARGES_SCHED\n\t,MGE1M AS DUN_CHARGE_1\n\t,MG1BL AS DOC_NO_CHARGE_1\n\t,MG1TY AS CHARGE_TYPE_1\n\t,POST1 AS UPDATE2\n\t,MGE2M AS DUN_CHARGE_2\n\t,MG2BL AS DOC_NO_C2\n\t,MG2TY AS CHARGE_TYPE_2\n\t,POST2 AS UPDATE1\n\t,MGE3M AS DUN_CHARGE_3\n\t,MG3BL AS DOC_NO_C3\n\t,MG3TY AS CHARGE_TYPE_3\n\t,POST3 AS UPDATE\n\t,MINTM AS DUNN_INT\n\t,MIBEL AS INTEREST_DOC\n\t,BONIT AS CREDITWTH\n\t,XMSTO AS DUNNING_CANCELED\n\t,NRZAS AS PYT_FORM_NO\n\t,XINFO AS INFO_GROUP\n\t,FRDAT AS PAYMENT_TARGET\n\t,COKEY AS CORRESPOND_KEY\n\t,XCOLL AS SUBMCOLLECTAGNY\n\t,RFZAS AS PMNT_FORM_REF\n\t,TODAT AS TO_DATE\n\t,STAKZ AS STATISTICAL_KEY\n\t,CHECKBOX AS TECHNICAL_FIELD\n\t,SUCPC AS SUCCESS\n\t,ABWTP AS DOC_CAT\n\t,ABWBL AS SUBSTITUTE_DOC\n\t,STUDT AS MAX_DEFERRAL\n\t,SUCDT AS DETERMINED_ON\n\t,SCDST AS SUCCESS_VALUATN\n\t,FORMKEY AS APPLICAT_FORM\n\t,ZZSHUTOFF_DATE AS SHUT_OFF_DATE\n\t,ZZBILL_MSG_TYP AS MESSAGE_TYPE\n\t,ZZPRINT_STAT AS PRINT_STATUS\n\t,ZZDAYS_TO_ADD AS DAYS_TO_ADD\n\t,ZZREVERSED_ON_DATE AS ZZREVERSED_ON_DATE\n\t,ZZREVERSED_ON_TIME AS ZZREVERSED_ON_TIME\n\t,ZZREVERSED_BY AS ZZREVERSED_BY\n\t,ZZ_REVERSAL_TCODE AS ZZ_REVERSAL_TCODE\n\t,SCORE AS VALUATION_NO\n\t,RANK AS RANKING\n\t,EPOCH_FLAG\n\t,EDA_CREATION_DATE\nFROM CODS_NONPROD.FKKMAKO;","commandVersion":1,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935096734,"submitTime":1663935096705,"finishTime":1663935097347,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"c6115a38-45d7-4034-a824-89239bad72ed"},{"version":"CommandV1","origId":1032317636649213,"guid":"326ad589-e8f3-4200-a9e6-b5f7ab693a62","subtype":"command","commandType":"auto","position":9.0,"command":"SELECT COUNT(*) , EPOCH_FLAG FROM edw_nonprod_cods.fkkmako GROUP BY EPOCH_FLAG","commandVersion":21,"state":"finished","results":{"type":"table","data":[[116846694,"CURRENT"]],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[{"name":"count(1)","type":"\"long\"","metadata":"{\"__autoGeneratedAlias\":\"true\"}"},{"name":"EPOCH_FLAG","type":"\"string\"","metadata":"{}"}],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935207803,"submitTime":1663935207770,"finishTime":1663935231885,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"288","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",1]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"ee4031d9-704e-4cb2-821f-416a15a11bbb"},{"version":"CommandV1","origId":1032317636649214,"guid":"68b47605-c840-4136-a234-7f2524efaf42","subtype":"command","commandType":"auto","position":10.0,"command":"SELECT COUNT(*) , EPOCH_FLAG FROM edw_nonprod_cods.BCONT GROUP BY EPOCH_FLAG","commandVersion":3,"state":"finished","results":{"type":"table","data":[[440229757,"CURRENT"],[195590,"ARCHIVE"]],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[{"name":"count(1)","type":"\"long\"","metadata":"{\"__autoGeneratedAlias\":\"true\"}"},{"name":"EPOCH_FLAG","type":"\"string\"","metadata":"{}"}],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935300779,"submitTime":1663935300722,"finishTime":1663935306455,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"288","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",2]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"0770ed21-3d7d-44b8-9ff6-1f404e9dddc4"},{"version":"CommandV1","origId":1032317636649215,"guid":"72bbb94a-3af1-4a40-86d3-947e2e13592a","subtype":"command","commandType":"auto","position":9.5,"command":"SELECT COUNT(*) , EPOCH_FLAG FROM edw_nonprod_cods.fkkmakT GROUP BY EPOCH_FLAG","commandVersion":2,"state":"finished","results":{"type":"table","data":[[1448791,"CURRENT"],[5586,"ARCHIVE"]],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[{"name":"count(1)","type":"\"long\"","metadata":"{\"__autoGeneratedAlias\":\"true\"}"},{"name":"EPOCH_FLAG","type":"\"string\"","metadata":"{}"}],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935329669,"submitTime":1663935329636,"finishTime":1663935336979,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"288","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",2]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"95ad34b6-0371-4c2c-804d-2041c8074758"},{"version":"CommandV1","origId":1032317636649216,"guid":"7a0b4d64-fd8c-4c7b-ac99-d9b4f89469bc","subtype":"command","commandType":"auto","position":11.0,"command":"SELECT COUNT(*) , EPOCH_FLAG FROM edw_nonprod_cods.dfkkcoll GROUP BY EPOCH_FLAG","commandVersion":8,"state":"finished","results":{"type":"table","data":[[63206966,"CURRENT"]],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[{"name":"count(1)","type":"\"long\"","metadata":"{\"__autoGeneratedAlias\":\"true\"}"},{"name":"EPOCH_FLAG","type":"\"string\"","metadata":"{}"}],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935392887,"submitTime":1663935392859,"finishTime":1663935422905,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"288","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",1]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"3dfdd8d4-53d3-4afa-a00d-4bdabc7c2e7a"},{"version":"CommandV1","origId":1032317636649217,"guid":"56d93f6c-c9ec-47e7-8c7a-66056542ca41","subtype":"command","commandType":"auto","position":12.0,"command":"SELECT COUNT(*) , EPOCH_FLAG FROM edw_nonprod_cods.dfkkcollh GROUP BY EPOCH_FLAG","commandVersion":2,"state":"finished","results":{"type":"table","data":[[299124775,"CURRENT"]],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[{"name":"count(1)","type":"\"long\"","metadata":"{\"__autoGeneratedAlias\":\"true\"}"},{"name":"EPOCH_FLAG","type":"\"string\"","metadata":"{}"}],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935448283,"submitTime":1663935448254,"finishTime":1663935466113,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"288","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",1]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"fc1ea7ab-bc34-4089-a684-196c74c42b12"},{"version":"CommandV1","origId":1032317636649218,"guid":"5349d4cb-d30b-4991-88b7-6ad7481dcd3d","subtype":"command","commandType":"auto","position":13.0,"command":"SELECT COUNT(*) , EPOCH_FLAG FROM edw_nonprod_cods.dfkklocks GROUP BY EPOCH_FLAG","commandVersion":4,"state":"finished","results":{"type":"table","data":[[83062711,"CURRENT"]],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[{"name":"count(1)","type":"\"long\"","metadata":"{\"__autoGeneratedAlias\":\"true\"}"},{"name":"EPOCH_FLAG","type":"\"string\"","metadata":"{}"}],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935488208,"submitTime":1663935488177,"finishTime":1663935489475,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"288","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",1]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"e829a6f5-0bb4-430f-be1a-bba742c11e56"},{"version":"CommandV1","origId":1032317636649219,"guid":"a94aad27-b532-41a0-abed-ca8c52121bbf","subtype":"command","commandType":"auto","position":14.0,"command":"SELECT COUNT(*) , EPOCH_FLAG FROM edw_nonprod_cods.dfkklocksh GROUP BY EPOCH_FLAG","commandVersion":2,"state":"finished","results":{"type":"table","data":[[38924839,"CURRENT"]],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[{"name":"count(1)","type":"\"long\"","metadata":"{\"__autoGeneratedAlias\":\"true\"}"},{"name":"EPOCH_FLAG","type":"\"string\"","metadata":"{}"}],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"workflows":[],"startTime":1663935505186,"submitTime":1663935505158,"finishTime":1663935506378,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"288","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",1]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"6f4fe4ba-51e1-402e-a426-92743f11cccb"}],"dashboards":[],"guid":"86edfebc-9b6a-4470-a5f9-b643a2622e2e","globalVars":{},"iPythonMetadata":null,"inputWidgets":{},"notebookMetadata":{"pythonIndentUnit":4},"reposExportFormat":"SOURCE"}