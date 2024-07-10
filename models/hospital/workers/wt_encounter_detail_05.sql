/*
This focuses on record ranking and handling null values:

1. Record Ranking:
   - Assigns a rank (REC_RANK) to each record within an ENCNT_SK group,
     ordered by VLD_FR_TS, MSG_CTRL_ID_TXT, and DW_INSRT_TS.
   - This ranking ensures a consistent order for processing records within each encounter.

2. Accommodation Reference Handling:
   - Implements a LAST_VALUE window function to carry forward the last non-null ACCOM_REF_CD
     within each ENCNT_SK group.
   - This ensures that each record has an accommodation reference, even if it's null in the current record.

3. EPIC-specific Logic:
   - For EPIC source systems, uses the carried-forward LAST_NON_NULL_ACCOM_REF_CD.
   - For other systems, uses the original ACCOM_REF_CD.

4. Column Selection:
   - Explicitly selects columns to ensure only relevant fields are included.
   - Some columns (e.g., ARRV_MODE_CD_SK, CNFD_CD_SK, EMR_PTNT_ID, SPCL_PGM_CD_SK) are commented out,
     possibly due to data quality issues or irrelevance in this context.

*/

WITH wt_encounter_detail_04 AS (
    SELECT * FROM {{ ref('wt_encounter_detail_04') }}
),

ranked_records AS (
    SELECT 
        ENCNT_SK,
        VLD_FR_TS,
        MSG_CTRL_ID_TXT,
        DW_INSRT_TS,
        ROW_NUMBER() OVER(
            PARTITION BY ENCNT_SK 
            ORDER BY VLD_FR_TS, MSG_CTRL_ID_TXT, DW_INSRT_TS
        ) AS REC_RANK,
        -- List all other columns explicitly here
        PATIENT_DW_ID,
        PAT_ACCT_NUM,
        COMPANY_CODE,
        COID,
        VLD_TO_TS,
        ENCNT_TS,
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
        MESSAGE_TYPE_TRIGGER_EVENT,
        MESSAGE_TYPE_TRIGGER_EVENT_DIS,
        MESSAGE_TYPE_TRIGGER_EVENT_INS
    FROM wt_encounter_detail_04
),

last_non_null_accom AS (
    SELECT
        ranked_records.*,
        LAST_VALUE(ACCOM_REF_CD IGNORE NULLS) OVER (
            PARTITION BY ENCNT_SK 
            ORDER BY VLD_FR_TS 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS LAST_NON_NULL_ACCOM_REF_CD
    FROM ranked_records
)

SELECT
    last_non_null_accom.REC_RANK,
    last_non_null_accom.ENCNT_SK,
    last_non_null_accom.VLD_FR_TS,
    last_non_null_accom.PATIENT_DW_ID,
    last_non_null_accom.PAT_ACCT_NUM,
    last_non_null_accom.COMPANY_CODE,
    last_non_null_accom.COID,
    last_non_null_accom.VLD_TO_TS,
    last_non_null_accom.ENCNT_TS,
   
    last_non_null_accom.ALT_ENCNT_TXT,
    last_non_null_accom.VST_TYPE_CD_SK,
    last_non_null_accom.EHR_MED_REC_NUM,
    last_non_null_accom.EHR_PAT_ACCT_NUM,
    last_non_null_accom.EHR_MED_URN,
    last_non_null_accom.PT_OF_ORIG_CD_SK,
   -- last_non_null_accom.ARRV_MODE_CD_SK,
    last_non_null_accom.ADMT_TYPE_REF_CD,
    last_non_null_accom.ADMT_TS,
    last_non_null_accom.ADMT_CMPLN_TXT,
    last_non_null_accom.RE_ADMT_IND,
    -- last_non_null_accom.CNFD_CD_SK,
    last_non_null_accom.HSPTL_SRVC_CD_SK,
    last_non_null_accom.EXPCT_NUM_OF_INS_PLAN_CNT,
    last_non_null_accom.ACTL_LEN_OF_STAY_DYS_CNT,
    last_non_null_accom.EMPMNT_ILLNS_IND,
    last_non_null_accom.ACDNT_AUTO_ST_CD,
    last_non_null_accom.ACDNT_TYPE_REF_CD,
    last_non_null_accom.ACDNT_TS,
    last_non_null_accom.DSCRG_TS,
    last_non_null_accom.DSCRG_STS_CD_SK,
    last_non_null_accom.PTNT_DTH_TS,
    last_non_null_accom.PTNT_DTH_IND,
    last_non_null_accom.PTNT_STS_REF_CD,
    last_non_null_accom.PTNT_CLASS_REF_CD,
    last_non_null_accom.PTNT_STS_EFFV_DT,
   -- last_non_null_accom.EMR_PTNT_ID,
    last_non_null_accom.EMR_PTNT_ID_ASSGN_AUTH,
   -- last_non_null_accom.SPCL_PGM_CD_SK,
    last_non_null_accom.PRTY_REF_CD,
    last_non_null_accom.SIG_DT,
    last_non_null_accom.ADV_DRTCV_PRSNT_TXT,
    last_non_null_accom.EFFV_FR_DT,
    last_non_null_accom.EFFV_TO_DT,
    last_non_null_accom.TYPE_REF_CD,
    CASE 
        WHEN last_non_null_accom.SRC_SYS_REF_CD = 'EPIC' THEN LAST_NON_NULL_ACCOM_REF_CD
        ELSE last_non_null_accom.ACCOM_REF_CD 
    END AS ACCOM_REF_CD,
    last_non_null_accom.SRC_SYS_REF_CD,
    last_non_null_accom.SRC_SYS_UNQ_KEY_TXT,
    last_non_null_accom.CRT_RUN_ID,
    last_non_null_accom.LST_UPDT_RUN_ID,
    last_non_null_accom.MSG_CTRL_ID_TXT,
    last_non_null_accom.MESSAGE_TYPE_TRIGGER_EVENT,
    last_non_null_accom.MESSAGE_TYPE_TRIGGER_EVENT_DIS,
    last_non_null_accom.MESSAGE_TYPE_TRIGGER_EVENT_INS,
    last_non_null_accom.DW_INSRT_TS
FROM last_non_null_accom
