/*
Model: wt_encounter_detail_01
Location: models/worker_transformations/wt_encounter_detail_01.sql

This is the first worker transformation model in the encounter detail processing pipeline.
It builds upon the int_encounter_detail model, applying initial transformations and filters:

1. Handles EPIC-specific logic for ENCNT_TS, ADMT_TS, and DSCRG_TS fields.
2. Adjusts DSCRG_STS_CD_SK for specific EPIC message types.
3. Calculates PTNT_STS_EFFV_DT from VLD_FR_TS.
4. Renames some fields for consistency (e.g., SOURCE_INTERFACE to SRC_SYS_REF_CD).
5. Filters out records with null ENCNT_SK.

Note: Some filtering conditions are currently commented out to allow data flow for testing.
In production, uncomment these filters to ensure data quality and consistency.

*/

WITH int_encounter_detail AS (
    SELECT * FROM {{ ref('int_encounter_detail') }}
)

SELECT
    ENCNT_SK,
    VLD_FR_TS,
    PATIENT_DW_ID,
    PAT_ACCT_NUM,
    COMPANY_CODE,
    COID,
    VLD_TO_TS,
    -- Handle ENCNT_TS based on source system
    CASE 
        WHEN SOURCE_INTERFACE = 'EPIC' AND TRIM(MESSAGE_TYPE_TRIGGER_EVENT) = 'A11' THEN NULL 
        ELSE ADMT_TS
    END AS ENCNT_TS,
    ALT_ENCNT_TXT,
    VST_TYPE_CD_SK,
    EHR_MED_REC_NUM,
    EHR_PAT_ACCT_NUM,
    EHR_MED_URN,
    PT_OF_ORIG_CD_SK,
    ADMT_TYPE_REF_CD,
    -- Handle ADMT_TS based on source system
    CASE 
        WHEN SOURCE_INTERFACE = 'EPIC' AND TRIM(MESSAGE_TYPE_TRIGGER_EVENT) = 'A11' THEN NULL 
        ELSE ADMT_TS
    END AS ADMT_TS,
    ADMT_CMPLN_TXT,
    RE_ADMT_IND,
    HSPTL_SRVC_CD_SK,
    EXPCT_NUM_OF_INS_PLAN_CNT,
    ACTL_LEN_OF_STAY_DYS_CNT,
    EMPMNT_ILLNS_IND,
    ACDNT_AUTO_ST_CD,
    ACDNT_TYPE_REF_CD,
    ACDNT_TS,
    -- Handle DSCRG_TS based on source system
    CASE 
        WHEN SOURCE_INTERFACE = 'EPIC' AND TRIM(MESSAGE_TYPE_TRIGGER_EVENT) IN ('A13','A01') THEN NULL 
        ELSE DSCRG_TS
    END AS DSCRG_TS,
    -- Handle DSCRG_STS_CD_SK based on source system
    CASE 
        WHEN SOURCE_INTERFACE = 'EPIC' AND TRIM(MESSAGE_TYPE_TRIGGER_EVENT) IN ('A13','A01') THEN 0 
        ELSE DSCRG_STS_CD_SK
    END AS DSCRG_STS_CD_SK,
    PTNT_DTH_TS,
    PTNT_DTH_IND,
    PTNT_STS_REF_CD,
    PTNT_CLASS_REF_CD,
    DATE(VLD_FR_TS) AS PTNT_STS_EFFV_DT,
    EMR_PTNT_ID_ASSGN_AUTH,
    ADMT_TYPE_REF_CD AS PRTY_REF_CD,  -- Assuming PRTY_REF_CD is the same as ADMT_TYPE_REF_CD
    SIG_DT,
    ADV_DRTCV_PRSNT_TXT,
    EFFV_FR_DT,
    EFFV_TO_DT,
    TYPE_REF_CD,
    ACCOM_REF_CD,
    SOURCE_INTERFACE AS SRC_SYS_REF_CD,
    SRC_SYS_UNQ_KEY_TXT,
    CRT_RUN_ID,
    LST_UPDT_RUN_ID,
    MESSAGE_CONTROL_ID AS MSG_CTRL_ID_TXT,
    MESSAGE_TYPE_TRIGGER_EVENT,
    DW_INSRT_TS
FROM int_encounter_detail
WHERE ENCNT_SK IS NOT NULL 
/*** only commenting out below lines so I have some data flow through! ***/
   -- AND PTNT_CLASS_REF_CD != 'SEG_NA' 
   -- AND EHR_PAT_ACCT_NUM IS NOT NULL
   -- AND VST_TYPE_CD_SK IS NOT NULL
   -- AND PT_OF_ORIG_CD_SK IS NOT NULL
   -- AND DSCRG_STS_CD_SK IS NOT NULL
   -- AND HSPTL_SRVC_CD_SK IS NOT NULL