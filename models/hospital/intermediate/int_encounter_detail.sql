-- models/intermediate/int_encounter_detail.sql

WITH encounter_base AS (
    SELECT * FROM {{ ref('int_encounter_base') }}
),

visit_info AS (
    SELECT * FROM {{ ref('int_visit_info') }}
),

visit_type_codes AS (
    SELECT * FROM {{ ref('int_visit_type_codes') }}
),

point_of_origin_codes AS (
    SELECT * FROM {{ ref('int_point_of_origin_codes') }}
),

discharge_status_codes AS (
    SELECT * FROM {{ ref('int_discharge_status_codes') }}
),

hospital_service_codes AS (
    SELECT * FROM {{ ref('int_hospital_service_codes') }}
),

additional_visit_info AS (
    SELECT * FROM {{ ref('int_additional_visit_info') }}
),

insurance_info AS (
    SELECT * FROM {{ ref('int_insurance_info') }}
),

observation_data AS (
    SELECT * FROM {{ ref('int_observation_data') }}
),

patient_info AS (
    SELECT * FROM {{ ref('int_patient_info') }}
),

facility_info AS (
    SELECT * FROM {{ ref('int_facility_info') }}
)

SELECT
    -- Encounter Base Information
    eb.ENCNT_SK,
    eb.MESSAGE_CONTROL_ID,
    eb.MESSAGE_DATE_TIME AS VLD_FR_TS,
    eb.PATIENT_DW_ID,
    eb.PATIENT_ACCOUNT_NUM AS PAT_ACCT_NUM,
    eb.COMPANY_CODE,
    eb.COID,
    '9999-12-31 00:00:00.000000' AS VLD_TO_TS,  -- This will be updated in worker transformations
    eb.MEDICAL_RECORD_NUM AS EHR_MED_REC_NUM,
    eb.PATIENT_ACCOUNT_NUM AS EHR_PAT_ACCT_NUM,
    CASE
        WHEN eb.SOURCE_INTERFACE = 'Cerner' AND eb.SENDING_FACILITY <> 'COCCMH' THEN pi.ALTERNATE_PATIENT_ID
        ELSE eb.MEDICAL_RECORD_URN
    END AS EHR_MED_URN,
    eb.SENDING_FACILITY,
    eb.SOURCE_INTERFACE,
    eb.MESSAGE_TYPE_TRIGGER_EVENT,

    -- Visit Information
    vi.PATIENT_CLASS_ID AS PTNT_CLASS_REF_CD,
    vi.ACCOUNT_STATUS_ID AS PTNT_STS_REF_CD,
    vi.ADMISSION_TYPE_ID AS ADMT_TYPE_REF_CD,
    vi.ADMIT_DATE_TIME AS ADMT_TS,
    vi.DISCHARGE_DATE_TIME AS DSCRG_TS,

    -- Visit Type Code
    vtc.CD_SK AS VST_TYPE_CD_SK,

    -- Point of Origin Code
    poc.CD_SK AS PT_OF_ORIG_CD_SK,

    -- Discharge Status Code
    dsc.CD_SK AS DSCRG_STS_CD_SK,

    -- Hospital Service Code
    hsc.CD_SK AS HSPTL_SRVC_CD_SK,

    -- Additional Visit Information
    avi.ADMIT_REASON_TEXT AS ADMT_CMPLN_TXT,
    avi.ACTUAL_LENGTH_OF_INPATIENT_STAY AS ACTL_LEN_OF_STAY_DYS_CNT,
    avi.ACCOMMODATION_CODE_ID AS ACCOM_REF_CD,

    -- Insurance Information
    ii.EXPCT_NUM_OF_INS_PLAN_CNT,

    -- Observation Data (Encounter ID from OBX)
    od.ENCOUNTER_ID AS ALT_ENCNT_TXT,

    -- Patient Information
    pi.PATIENT_DEATH_DATE_AND_TIME AS PTNT_DTH_TS,
    pi.PATIENT_DEATH_INDICATOR AS PTNT_DTH_IND,

    -- Facility Information
    fi.EMR_PTNT_ID_ASSGN_AUTH,

    -- Additional fields (placeholders or derived)
    NULL AS RE_ADMT_IND,
    NULL AS EMPMNT_ILLNS_IND,
    NULL AS ACDNT_AUTO_ST_CD,
    NULL AS ACDNT_TYPE_REF_CD,
    NULL AS ACDNT_TS,
    NULL AS SIG_DT,
    NULL AS ADV_DRTCV_PRSNT_TXT,
    NULL AS EFFV_FR_DT,
    NULL AS EFFV_TO_DT,
    'ENCNT_DTL' AS TYPE_REF_CD,
    eb.ENCNT_SSUKT AS SRC_SYS_UNQ_KEY_TXT,
    100 AS CRT_RUN_ID,
    100 AS LST_UPDT_RUN_ID,

    -- Metadata
    CURRENT_TIMESTAMP AS DW_INSRT_TS

FROM encounter_base eb
LEFT JOIN visit_info vi
    ON eb.MESSAGE_CONTROL_ID = vi.MESSAGE_CONTROL_ID
LEFT JOIN visit_type_codes vtc
    ON vi.VST_TYPE_CD_SSUKT = vtc.CD_SSUKT
LEFT JOIN point_of_origin_codes poc
    ON vi.PT_OF_ORIG_CD_SSUKT = poc.CD_SSUKT
LEFT JOIN discharge_status_codes dsc
    ON vi.DSCRG_STS_CD_SSUKT = dsc.CD_SSUKT
LEFT JOIN hospital_service_codes hsc
    ON vi.HSPTL_SRVC_CD_SSUKT = hsc.CD_SSUKT
LEFT JOIN additional_visit_info avi
    ON eb.MESSAGE_CONTROL_ID = avi.MESSAGE_CONTROL_ID
LEFT JOIN insurance_info ii
    ON eb.MESSAGE_CONTROL_ID = ii.MESSAGE_CONTROL_ID
LEFT JOIN observation_data od
    ON eb.MESSAGE_CONTROL_ID = od.MESSAGE_CONTROL_ID
LEFT JOIN patient_info pi
    ON eb.MESSAGE_CONTROL_ID = pi.MESSAGE_CONTROL_ID
LEFT JOIN facility_info fi
    ON eb.SENDING_FACILITY = fi.FACILITY_MNEMONIC_CS

/*
 * Additional filters can be added here if needed, for example:
 * WHERE eb.MESSAGE_TYPE = 'ADT'
 *   AND eb.MESSAGE_DATE_TIME <> '0'
 *   AND eb.MESSAGE_TYPE_TRIGGER_EVENT NOT IN ('A17','A29','A31','A60')
 */