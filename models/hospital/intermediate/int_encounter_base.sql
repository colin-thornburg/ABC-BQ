/*

This intermediate model combines HL7 message header (MSH) data with clinical keys.
It filters for ADT (Admit/Discharge/Transfer) messages from EPIC,
excluding specific message types. The model joins MSH and clinical keys data
based on patient account number and sending facility, creating a base
for encounter-level information. It includes encounter-specific identifiers
and patient demographics.
*/

WITH stg_msh AS (
    SELECT * FROM {{ ref('stg_hl7_msh') }}
),

stg_clinical_keys AS (
    SELECT * FROM {{ ref('stg_hl7_clinical_keys') }}
)

SELECT
    msh.*,  -- Placeholder for specific column selection
    clac.ENCNT_SK,
    clac.PATIENT_DW_ID,
    clac.COMPANY_CODE,
    clac.COID,
    clac.ENCNT_SSUKT
    -- Add other relevant columns from clinical_keys
FROM stg_msh AS msh
LEFT JOIN stg_clinical_keys AS clac
    ON TRIM(msh.PATIENT_ACCOUNT_NUM) = TRIM(clac.PATIENT_ACCOUNT_NUM_RAW)
    AND TRIM(msh.SENDING_FACILITY) = TRIM(clac.SENDING_FACILITY)
WHERE
    msh.MESSAGE_TYPE = 'ADT' 
    AND SAFE_CAST(msh.MESSAGE_DATE_TIME AS DATETIME) IS NOT NULL
    AND msh.MESSAGE_TYPE_TRIGGER_EVENT NOT IN ('A17','A29','A31','A60')
    AND msh.SOURCE_INTERFACE = 'EPIC'