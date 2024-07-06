-- models/staging/stg_hl7_msh.sql

WITH source AS (
    SELECT * FROM {{ source('edwhl7_staging_views', 'HL7_MSH') }}
),

renamed AS (
    SELECT
        MESSAGE_CONTROL_ID,
        MESSAGE_DATE_TIME,
        PATIENT_ACCOUNT_NUM,
        MEDICAL_RECORD_NUM,
        MEDICAL_RECORD_URN,
        SENDING_FACILITY,
        SOURCE_INTERFACE,
        MESSAGE_TYPE,
        MESSAGE_TYPE_TRIGGER_EVENT,
        -- Other columns would be listed here
        -- ...
        {{ dbt_utils.generate_surrogate_key([
            'MESSAGE_CONTROL_ID', 
            'SENDING_FACILITY', 
            'MESSAGE_DATE_TIME'
        ]) }} AS MSH_HASH_KEY,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_HL7_MSH_HASHID
    FROM source
)

SELECT * FROM renamed