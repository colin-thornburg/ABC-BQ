-- models/staging/stg_hl7_pv1.sql

WITH source AS (
    SELECT * FROM {{ source('edwhl7_staging_views', 'HL7_PV1') }}
),

renamed AS (
    SELECT
        MESSAGE_CONTROL_ID,
        PATIENT_TYPE_ID,
        ADMIT_SOURCE_ID,
        MODE_OF_ARRIVAL,
        ADMISSION_TYPE_ID,
        ADMIT_DATE_TIME,
        DISCHARGE_DATE_TIME,
        DISCHARGE_DISPOSITION_ID,
        ACCOUNT_STATUS_ID,
        FINANCIAL_CLASS_FINANCIAL_CLASS_CODE_ID,
        SENDING_FACILITY,
        PATIENT_CLASS_ID,
        ARRV_MODE_CD_SSUKT,
        PT_OF_ORIG_CD_SSUKT,
        DSCRG_STS_CD_SSUKT,
        VST_TYPE_CD_SSUKT,
        SPCL_PGM_CD_SSUKT,
        HSPTL_SRVC_CD_SSUKT,
        CONF_CD_SSUKT,
        VISIT_NUM_ID_NUM,
        MESSAGE_TYPE,
        {{ dbt_utils.generate_surrogate_key([
            'MESSAGE_CONTROL_ID', 
            'SENDING_FACILITY'
        ]) }} AS PV1_HASH_KEY,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_HL7_PV1_HASHID
    FROM source
    WHERE MESSAGE_TYPE = 'ADT'
)

SELECT * FROM renamed