-- models/worker_transformations/wt_encounter_detail_07.sql

WITH wt_encounter_detail_06 AS (
    SELECT * FROM {{ ref('wt_encounter_detail_06') }}
),

dense_ranked AS (
    SELECT 
        *,
        DENSE_RANK() OVER (
            PARTITION BY ENCNT_SK 
            ORDER BY VLD_FR_TS
        ) AS DENSE_REC_RANK
    FROM wt_encounter_detail_06
    WHERE IS_CHANGED = 1  -- Only include records that have changed
)

SELECT
    DENSE_REC_RANK,
    ENCNT_SK,
    VLD_FR_TS,
    PATIENT_DW_ID,
    PAT_ACCT_NUM,
    COMPANY_CODE,
    COID,
    VLD_TO_TS,
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
    DW_INSRT_TS,
    IS_CHANGED
FROM dense_ranked