-- models/staging/stg_hl7_in1.sql

WITH source AS (
    SELECT * FROM {{ source('edwhl7_staging_views', 'HL7_IN1') }}
),

renamed AS (
    /*
    This CTE renames and selects the columns we need from the source table.
    We're keeping all relevant fields that might be used in downstream models.
    */
    SELECT
        MESSAGE_CONTROL_ID,
        SET_ID,
        INSURANCE_PLAN_ID,
        INSURANCE_COMPANY_ID,
        INSURANCE_COMPANY_NAME,
        INSURANCE_COMPANY_ADDRESS,
        INSURANCE_CO_CONTACT_PERSON,
        INSURANCE_CO_PHONE_NUMBER,
        GROUP_NAME,
        GROUP_NUMBER,
        PLAN_EFFECTIVE_DATE,
        PLAN_EXPIRATION_DATE,
        POLICY_NUMBER,
        POLICY_TYPE,
        MESSAGE_TYPE,
        {{ dbt_utils.generate_surrogate_key([
            'MESSAGE_CONTROL_ID', 
            'SET_ID'
        ]) }} AS IN1_HASH_KEY,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_HL7_IN1_HASHID
    FROM source
    WHERE MESSAGE_TYPE = 'ADT'  -- We're only interested in ADT messages
)

SELECT * FROM renamed