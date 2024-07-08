-- models/staging/stg_hl7_clinical_keys.sql

WITH source AS (
    SELECT * 
   -- FROM {{ source('edwhl7_staging_views', 'HL7_CLINICAL_KEYS') }}
    FROM {{ ref('hl7_clinical_keys') }}
),

renamed AS (
    SELECT
        ENCNT_SK,
        PATIENT_DW_ID,
        PATIENT_ACCOUNT_NUM_RAW,
        COMPANY_CODE,
        COID,
        ENCNT_SSUKT,
        SENDING_FACILITY,
        -- Other columns would be listed here
        -- ...
        {{ dbt_utils.generate_surrogate_key([
            'ENCNT_SK', 
            'PATIENT_ACCOUNT_NUM_RAW', 
            'SENDING_FACILITY'
        ]) }} AS CLINICAL_KEYS_HASH_KEY,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_HL7_CLINICAL_KEYS_HASHID
    FROM source
)

SELECT * FROM renamed