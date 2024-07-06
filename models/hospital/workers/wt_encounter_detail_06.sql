-- models/worker_transformations/wt_encounter_detail_06.sql

WITH wt_encounter_detail_05 AS (
    SELECT * FROM {{ ref('wt_encounter_detail_05') }}
),

prev_record AS (
    SELECT 
        *,
        LAG(VST_TYPE_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_VST_TYPE_CD_SK,
        LAG(PT_OF_ORIG_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_PT_OF_ORIG_CD_SK,
        LAG(ARRV_MODE_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ARRV_MODE_CD_SK,
        LAG(ENCNT_TS) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ENCNT_TS,
        LAG(ADMT_TS) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ADMT_TS,
        LAG(ADMT_CMPLN_TXT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ADMT_CMPLN_TXT,
        LAG(RE_ADMT_IND) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_RE_ADMT_IND,
        LAG(CNFD_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_CNFD_CD_SK,
        LAG(HSPTL_SRVC_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_HSPTL_SRVC_CD_SK,
        LAG(EXPCT_NUM_OF_INS_PLAN_CNT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_EXPCT_NUM_OF_INS_PLAN_CNT,
        LAG(ACTL_LEN_OF_STAY_DYS_CNT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ACTL_LEN_OF_STAY_DYS_CNT,
        LAG(DSCRG_TS) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_DSCRG_TS,
        LAG(DSCRG_STS_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_DSCRG_STS_CD_SK,
        LAG(PTNT_STS_REF_CD) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_PTNT_STS_REF_CD,
        LAG(EMR_PTNT_ID_ASSGN_AUTH) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_EMR_PTNT_ID_ASSGN_AUTH,
        LAG(EMR_PTNT_ID) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_EMR_PTNT_ID,
        LAG(ADV_DRTCV_PRSNT_TXT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ADV_DRTCV_PRSNT_TXT,
        LAG(ACCOM_REF_CD) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ACCOM_REF_CD,
        LAG(ALT_ENCNT_TXT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ALT_ENCNT_TXT
    FROM wt_encounter_detail_05
)

SELECT 
    *,
    CASE
        WHEN REC_RANK = 1 THEN 1
        WHEN COALESCE(VST_TYPE_CD_SK, -1) != COALESCE(PREV_VST_TYPE_CD_SK, -1) THEN 1
        WHEN COALESCE(PT_OF_ORIG_CD_SK, -1) != COALESCE(PREV_PT_OF_ORIG_CD_SK, -1) THEN 1
        WHEN COALESCE(ARRV_MODE_CD_SK, -1) != COALESCE(PREV_ARRV_MODE_CD_SK, -1) THEN 1
        WHEN COALESCE(ENCNT_TS, '1900-01-01') != COALESCE(PREV_ENCNT_TS, '1900-01-01') THEN 1
        WHEN COALESCE(ADMT_TS, '1900-01-01') != COALESCE(PREV_ADMT_TS, '1900-01-01') THEN 1
        WHEN COALESCE(ADMT_CMPLN_TXT, '') != COALESCE(PREV_ADMT_CMPLN_TXT, '') THEN 1
        WHEN COALESCE(RE_ADMT_IND, '') != COALESCE(PREV_RE_ADMT_IND, '') THEN 1
        WHEN COALESCE(CNFD_CD_SK, -1) != COALESCE(PREV_CNFD_CD_SK, -1) THEN 1
        WHEN COALESCE(HSPTL_SRVC_CD_SK, -1) != COALESCE(PREV_HSPTL_SRVC_CD_SK, -1) THEN 1
        WHEN COALESCE(EXPCT_NUM_OF_INS_PLAN_CNT, -1) != COALESCE(PREV_EXPCT_NUM_OF_INS_PLAN_CNT, -1) THEN 1
        WHEN COALESCE(ACTL_LEN_OF_STAY_DYS_CNT, -1) != COALESCE(PREV_ACTL_LEN_OF_STAY_DYS_CNT, -1) THEN 1
        WHEN COALESCE(DSCRG_TS, '1900-01-01') != COALESCE(PREV_DSCRG_TS, '1900-01-01') THEN 1
        WHEN COALESCE(DSCRG_STS_CD_SK, -1) != COALESCE(PREV_DSCRG_STS_CD_SK, -1) THEN 1
        WHEN COALESCE(PTNT_STS_REF_CD, '') != COALESCE(PREV_PTNT_STS_REF_CD, '') THEN 1
        WHEN COALESCE(EMR_PTNT_ID_ASSGN_AUTH, '') != COALESCE(PREV_EMR_PTNT_ID_ASSGN_AUTH, '') THEN 1
        WHEN COALESCE(EMR_PTNT_ID, '') != COALESCE(PREV_EMR_PTNT_ID, '') THEN 1
        WHEN COALESCE(ADV_DRTCV_PRSNT_TXT, '') != COALESCE(PREV_ADV_DRTCV_PRSNT_TXT, '') THEN 1
        WHEN COALESCE(ACCOM_REF_CD, '') != COALESCE(PREV_ACCOM_REF_CD, '') THEN 1
        WHEN COALESCE(ALT_ENCNT_TXT, '') != COALESCE(PREV_ALT_ENCNT_TXT, '') THEN 1
        ELSE 0
    END AS IS_CHANGED
FROM prev_record