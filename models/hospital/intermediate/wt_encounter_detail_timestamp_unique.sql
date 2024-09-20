-- models/encounter_detail/wt_encounter_detail_timestamp_unique.sql

{{ config(materialized='view', tags=['timestamp']) }}

WITH int_encounter_detail AS (
    SELECT * FROM {{ ref('int_encounter_detail') }}
),

unique_timestamps AS (
    SELECT 
        *,
        -- Create a unique timestamp by adding microseconds based on the row number
        TIMESTAMP_ADD(
            TIMESTAMP_TRUNC(VLD_FR_TS, SECOND),
            INTERVAL (ROW_NUMBER() OVER(
                PARTITION BY ENCNT_SK, 
                TIMESTAMP_TRUNC(VLD_FR_TS, SECOND)  -- Truncate to seconds for partitioning
                ORDER BY MESSAGE_CONTROL_ID, DW_INSRT_TS
            ) - 1) MICROSECOND
        ) AS UNIQUE_VLD_FR_TS
    FROM int_encounter_detail
)

SELECT
    ENCNT_SK,
    UNIQUE_VLD_FR_TS AS VLD_FR_TS,
    PATIENT_DW_ID,
    PAT_ACCT_NUM,
    COMPANY_CODE,
    COID,
    VLD_TO_TS,
    ADMT_TS AS ENCNT_TS,  -- Using ADMT_TS as ENCNT_TS as per original model 01
    ALT_ENCNT_TXT,
    VST_TYPE_CD_SK,
    EHR_MED_REC_NUM,
    EHR_PAT_ACCT_NUM,
    EHR_MED_URN,
    PT_OF_ORIG_CD_SK,
    ADMT_TYPE_REF_CD,
    ADMT_TS,
    ADMT_CMPLN_TXT,
    RE_ADMT_IND,
    HSPTL_SRVC_CD_SK,
    EXPCT_NUM_OF_INS_PLAN_CNT,
    ACTL_LEN_OF_STAY_DYS_CNT,
    EMPMNT_ILLNS_IND,
    ACDNT_AUTO_ST_CD,
    ACDNT_TYPE_REF_CD,
    ACDNT_TS,
    DSCRG_TS,
    DSCRG_STS_CD_SK,
    PTNT_DTH_TS,
    PTNT_DTH_IND,
    PTNT_STS_REF_CD,
    PTNT_CLASS_REF_CD,
    DATE(UNIQUE_VLD_FR_TS) AS PTNT_STS_EFFV_DT,
    EMR_PTNT_ID_ASSGN_AUTH,
    ADMT_TYPE_REF_CD AS PRTY_REF_CD,  -- Using ADMT_TYPE_REF_CD as PRTY_REF_CD as per original model 01
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
FROM unique_timestamps