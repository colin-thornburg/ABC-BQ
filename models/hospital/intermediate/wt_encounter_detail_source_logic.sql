-- models/encounter_detail/wt_encounter_detail_source_logic.sql

{{ config(materialized='view', tags=['source_logic']) }}

WITH int_encounter_detail AS (
    SELECT * FROM {{ ref('int_encounter_detail') }}
),

-- CTE for initial transformations (from original model 01)
initial_transform AS (
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
        ADMT_TYPE_REF_CD AS PRTY_REF_CD,
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
),

-- CTE for additional transformations (from original model 02)
additional_transform AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY ENCNT_SK 
            ORDER BY VLD_FR_TS, MSG_CTRL_ID_TXT, DW_INSRT_TS
        ) AS REC_RANK,
        CASE 
            WHEN DSCRG_TS IS NULL THEN 'A13' 
            ELSE 'A03' 
        END AS MESSAGE_TYPE_TRIGGER_EVENT_DIS,
        CASE 
            WHEN TRIM(MESSAGE_TYPE_TRIGGER_EVENT) = 'A03' THEN MESSAGE_TYPE_TRIGGER_EVENT 
            ELSE 'A01' 
        END AS MESSAGE_TYPE_TRIGGER_EVENT_INS
    FROM initial_transform
),

-- CTE for source-specific logic (from original model 03)
source_specific_logic AS (
    SELECT
        a.*,
        CASE 
            WHEN a.SRC_SYS_REF_CD = 'EPIC' AND CAST(a.COID AS STRING) = '26960' THEN COALESCE(e.ENCNT_TS, a.ENCNT_TS)
            ELSE a.ENCNT_TS
        END AS UPDATED_ENCNT_TS,
        CASE 
            WHEN a.SRC_SYS_REF_CD = 'EPIC' AND CAST(a.COID AS STRING) = '26960' THEN COALESCE(e.ADMT_TS, a.ADMT_TS)
            ELSE a.ADMT_TS
        END AS UPDATED_ADMT_TS,
        CASE 
            WHEN a.SRC_SYS_REF_CD = 'EPIC' AND CAST(a.COID AS STRING) = '26960' THEN COALESCE(e.DSCRG_TS, a.DSCRG_TS)
            ELSE a.DSCRG_TS
        END AS UPDATED_DSCRG_TS,
        CASE 
            WHEN a.SRC_SYS_REF_CD = 'EPIC' AND CAST(a.COID AS STRING) = '26960' THEN COALESCE(e.DSCRG_STS_CD_SK, a.DSCRG_STS_CD_SK)
            ELSE a.DSCRG_STS_CD_SK
        END AS UPDATED_DSCRG_STS_CD_SK,
        CASE 
            WHEN a.SRC_SYS_REF_CD IN ('MEDITECH 6.0', 'EXPANSE') THEN COALESCE(m.EXPCT_NUM_OF_INS_PLAN_CNT, a.EXPCT_NUM_OF_INS_PLAN_CNT)
            ELSE a.EXPCT_NUM_OF_INS_PLAN_CNT
        END AS UPDATED_EXPCT_NUM_OF_INS_PLAN_CNT
    FROM additional_transform a
    LEFT JOIN (
        SELECT
            ENCNT_SK,
            ENCNT_TS,
            ADMT_TS,
            DSCRG_TS,
            DSCRG_STS_CD_SK
        FROM additional_transform
        WHERE SRC_SYS_REF_CD = 'EPIC' AND CAST(COID AS STRING) = '26960'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ENCNT_SK ORDER BY VLD_FR_TS DESC, MSG_CTRL_ID_TXT DESC, REC_RANK DESC, DW_INSRT_TS DESC) = 1
    ) e ON a.ENCNT_SK = e.ENCNT_SK
    LEFT JOIN (
        SELECT
            ENCNT_SK,
            EXPCT_NUM_OF_INS_PLAN_CNT
        FROM additional_transform
        WHERE SRC_SYS_REF_CD IN ('MEDITECH 6.0', 'EXPANSE')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ENCNT_SK ORDER BY VLD_FR_TS DESC, MSG_CTRL_ID_TXT DESC, REC_RANK DESC, DW_INSRT_TS DESC) = 1
    ) m ON a.ENCNT_SK = m.ENCNT_SK
)

SELECT
    ENCNT_SK,
    VLD_FR_TS,
    PATIENT_DW_ID,
    PAT_ACCT_NUM,
    COMPANY_CODE,
    COID,
    VLD_TO_TS,
    UPDATED_ENCNT_TS AS ENCNT_TS,
    ALT_ENCNT_TXT,
    VST_TYPE_CD_SK,
    EHR_MED_REC_NUM,
    EHR_PAT_ACCT_NUM,
    EHR_MED_URN,
    PT_OF_ORIG_CD_SK,
    ADMT_TYPE_REF_CD,
    UPDATED_ADMT_TS AS ADMT_TS,
    ADMT_CMPLN_TXT,
    RE_ADMT_IND,
    HSPTL_SRVC_CD_SK,
    UPDATED_EXPCT_NUM_OF_INS_PLAN_CNT AS EXPCT_NUM_OF_INS_PLAN_CNT,
    ACTL_LEN_OF_STAY_DYS_CNT,
    EMPMNT_ILLNS_IND,
    ACDNT_AUTO_ST_CD,
    ACDNT_TYPE_REF_CD,
    ACDNT_TS,
    UPDATED_DSCRG_TS AS DSCRG_TS,
    UPDATED_DSCRG_STS_CD_SK AS DSCRG_STS_CD_SK,
    PTNT_DTH_TS,
    PTNT_DTH_IND,
    PTNT_STS_REF_CD,
    PTNT_CLASS_REF_CD,
    PTNT_STS_EFFV_DT,
    EMR_PTNT_ID_ASSGN_AUTH,
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
    REC_RANK
FROM source_specific_logic