{{
    config(
        materialized='incremental',
        unique_key=['ENCNT_SK', 'VLD_FR_TS'],
        incremental_strategy='merge',
        on_schema_change='fail'
    )
}}

/*

Purpose:
This is the final model in the encounter detail processing pipeline, serving as the 
source of truth for encounter information in the data warehouse. It consolidates the data from various healthcare 
systems, ready for analytics and reporting.

Key Features:
1. Incremental Loading: Efficiently processes only new or changed data since the last run.
2. Historical Tracking: Maintains historical changes through VLD_FR_TS and VLD_TO_TS.
3. Data Quality: Includes only essential, validated fields from the source systems.
4. Performance Optimization: Configured for optimal query performance with appropriate 
   sort and distribution keys.

Usage:
This model should be the primary source for all encounter-related analytics and reporting.
It provides a comprehensive view of patient encounters across different healthcare systems
and facilities.

Incrementality:
- The model uses a merge strategy to update existing records and insert new ones.
- It's keyed on both ENCNT_SK and VLD_FR_TS to handle multiple versions of an encounter.
- The 'on_schema_change' option ensures the model adapts to upstream schema changes.

Note: Some fields (e.g., ARRV_MODE_CD_SK, CNFD_CD_SK) are commented out just because fake data generated had some limitations.
*/

WITH final_encounter_data AS (
    SELECT * FROM {{ ref('wt_encounter_detail_09') }}
)

SELECT
    ENCNT_SK,
    VLD_FR_TS,
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
    MSG_CTRL_ID_TXT,
    HL7_EV_ID,
    DW_INSRT_TS
FROM final_encounter_data

{% if is_incremental() %}
    WHERE VLD_FR_TS > (SELECT MAX(VLD_FR_TS) FROM {{ this }})
{% endif %}