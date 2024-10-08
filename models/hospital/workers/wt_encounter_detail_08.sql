-- models/worker_transformations/wt_encounter_detail_08.sql

WITH wt_encounter_detail_07 AS (
    SELECT * FROM {{ ref('wt_encounter_detail_07') }}
),

next_valid_from AS (
    SELECT 
        *,
        LEAD(VLD_FR_TS) OVER (
            PARTITION BY ENCNT_SK 
            ORDER BY DENSE_REC_RANK
        ) AS NEXT_VLD_FR_TS
    FROM wt_encounter_detail_07
)

SELECT
    ENCNT_SK,
    VLD_FR_TS,
    PATIENT_DW_ID,
    PAT_ACCT_NUM,
    COMPANY_CODE,
    COID,
    COALESCE(
        DATETIME(TIMESTAMP_SUB(NEXT_VLD_FR_TS, INTERVAL 1 MICROSECOND)), 
        DATETIME '9999-12-31 23:59:59.999999'
    ) AS VLD_TO_TS,
    ENCNT_TS,
    -- ENCNT_ID_TXT,
    ALT_ENCNT_TXT,
    VST_TYPE_CD_SK,
    EHR_MED_REC_NUM,
    EHR_PAT_ACCT_NUM,
    EHR_MED_URN,
    PT_OF_ORIG_CD_SK,
    -- ARRV_MODE_CD_SK,
    ADMT_TYPE_REF_CD,
    ADMT_TS,
    ADMT_CMPLN_TXT,
    RE_ADMT_IND,
    -- CNFD_CD_SK,
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
    PTNT_STS_EFFV_DT,
    -- EMR_PTNT_ID,
    EMR_PTNT_ID_ASSGN_AUTH,
    -- SPCL_PGM_CD_SK,
    PRTY_REF_CD,
    SIG_DT,
    ADV_DRTCV_PRSNT_TXT,
    EFFV_FR_DT,
    EFFV_TO_DT,
    TYPE_REF_CD,
    ACCOM_REF_CD,
    SRC_SYS_REF_CD,
    SRC_SYS_UNQ_KEY_TXT,
    CRT_RUN_ID,
    LST_UPDT_RUN_ID,
    MSG_CTRL_ID_TXT,
    MESSAGE_TYPE_TRIGGER_EVENT,
    MESSAGE_TYPE_TRIGGER_EVENT_DIS,
    MESSAGE_TYPE_TRIGGER_EVENT_INS,
    DW_INSRT_TS
FROM next_valid_from