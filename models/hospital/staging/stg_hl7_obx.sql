-- models/staging/stg_hl7_obx.sql

WITH source AS (
    SELECT * 
    -- FROM {{ source('edwhl7_staging_views', 'HL7_OBX') }}
    from {{ ref('hl7_obx') }}
),

renamed AS (
    /*
    This CTE renames and selects the columns we need from the source table.
    We're keeping all relevant fields that might be used in downstream models.
    */
    SELECT
        MESSAGE_CONTROL_ID,
        SET_ID,
        VALUE_TYPE,
        OBSV_ID_ID,
        OBSV_ID_TEXT,
        OBSV_ID_CODING_SYSTEM,
        OBSV_SUB_ID,
        OBSV_VALUE_TEXT,
        OBSV_VALUE_NUMERIC,
        UNITS,
        REFERENCE_RANGE,
        ABNORMAL_FLAGS,
        PROBABILITY,
        NATURE_OF_ABNORMAL_TEST,
        OBSV_RESULT_STATUS,
        EFFECTIVE_DATE_OF_REFERENCE_RANGE,
        USER_DEFINED_ACCESS_CHECKS,
        DATE_TIME_OF_THE_OBSERVATION,
        PRODUCERS_ID,
        RESPONSIBLE_OBSERVER,
        SOURCE_INTERFACE,
        MESSAGE_TYPE,
        {{ dbt_utils.generate_surrogate_key([
            'MESSAGE_CONTROL_ID', 
            'SET_ID',
            'OBSV_ID_ID'
        ]) }} AS OBX_HASH_KEY,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_HL7_OBX_HASHID
    FROM source
    WHERE MESSAGE_TYPE = 'ADT'  -- We're only interested in ADT messages
)

SELECT * FROM renamed