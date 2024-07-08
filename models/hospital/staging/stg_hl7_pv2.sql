-- models/staging/stg_hl7_pv2.sql

WITH source AS (
    SELECT * 
    -- FROM {{ source('edwhl7_staging_views', 'HL7_PV2') }}
    from {{ ref('hl7_pv2') }}
),

renamed AS (
    /*
    This CTE renames and selects the columns we need from the source table.
    We're keeping all relevant fields that might be used in downstream models.
    */
    SELECT
        MESSAGE_CONTROL_ID,
        ADMIT_REASON_ID,
        ADMIT_REASON_TEXT,
        ACTUAL_LENGTH_OF_INPATIENT_STAY,
        ARRV_MODE_CD_SSUKT,
        SOURCE_INTERFACE,
        ACCOMMODATION_CODE_ID,
        MESSAGE_TYPE,
        {{ dbt_utils.generate_surrogate_key([
            'MESSAGE_CONTROL_ID', 
            'SOURCE_INTERFACE'
        ]) }} AS PV2_HASH_KEY,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_HL7_PV2_HASHID
    FROM source
    WHERE MESSAGE_TYPE = 'ADT'  -- We're only interested in ADT messages
)

SELECT * FROM renamed