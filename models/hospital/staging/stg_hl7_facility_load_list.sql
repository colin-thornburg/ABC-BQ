-- models/staging/stg_hl7_facility_load_list.sql

WITH source AS (
    SELECT * 
    --FROM {{ source('edwcdm_views', 'HL7_FACILITY_LOAD_LIST') }}
    FROM {{ ref('hl7_facility_load_list') }}
),

renamed AS (
    /*
    This CTE renames and selects the columns we need from the source table.
    We're keeping all relevant fields that might be used in downstream models.
    */
    SELECT
        FACILITY_MNEMONIC_CS,
        NETWORK_MNEMONIC_CS,
        FACILITY_NAME,
        NETWORK_NAME,
        FACILITY_TYPE,
        FACILITY_ADDRESS,
        FACILITY_CITY,
        FACILITY_STATE,
        FACILITY_ZIP,
        FACILITY_COUNTY,
        FACILITY_COUNTRY,
        ACTIVE_FLAG,
        {{ dbt_utils.generate_surrogate_key([
            'FACILITY_MNEMONIC_CS', 
            'NETWORK_MNEMONIC_CS'
        ]) }} AS FACILITY_HASH_KEY,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_HL7_FACILITY_LOAD_LIST_HASHID
    FROM source
    WHERE ACTIVE_FLAG = 'Y'  -- We're only interested in active facilities
)

SELECT * FROM renamed