-- models/intermediate/int_facility_info.sql

WITH stg_facility AS (
    SELECT * FROM {{ ref('stg_hl7_facility_load_list') }}
),

facility_info AS (
    /*
    This CTE prepares the facility information.
    We're creating the EMR_PTNT_ID_ASSGN_AUTH field based on the 
    FACILITY_MNEMONIC_CS and NETWORK_MNEMONIC_CS, as per the original script.
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
        CASE 
            WHEN FACILITY_MNEMONIC_CS = 'COCUH' THEN 'FLBU'
            ELSE NETWORK_MNEMONIC_CS
        END AS EMR_PTNT_ID_ASSGN_AUTH,
        FACILITY_HASH_KEY
    FROM stg_facility
)

SELECT *
FROM facility_info