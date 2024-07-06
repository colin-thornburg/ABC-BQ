-- models/intermediate/int_encounter_base.sql

WITH stg_msh AS (
    SELECT * FROM {{ ref('stg_hl7_msh') }}
),

stg_clinical_keys AS (
    SELECT * FROM {{ ref('stg_hl7_clinical_keys') }}
)

SELECT
    msh.*,  -- Placeholder for specific column selection
    ck.ENCNT_SK,
    ck.PATIENT_DW_ID,
    ck.COMPANY_CODE,
    ck.COID,
    ck.ENCNT_SSUKT
    -- Add other relevant columns from clinical_keys
FROM stg_msh AS msh
LEFT JOIN stg_clinical_keys AS ck
    ON TRIM(msh.PATIENT_ACCOUNT_NUM) = TRIM(ck.PATIENT_ACCOUNT_NUM_RAW)
    AND TRIM(msh.SENDING_FACILITY) = TRIM(ck.SENDING_FACILITY)
WHERE
    msh.MESSAGE_TYPE = 'ADT' 
    AND msh.MESSAGE_DATE_TIME <> '0'
    AND msh.MESSAGE_TYPE_TRIGGER_EVENT NOT IN ('A17','A29','A31','A60')
    AND MSH.SOURCE_INTERFACE = 'EPIC' 