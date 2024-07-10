-- models/intermediate/int_additional_visit_info.sql

WITH stg_pv2 AS (
    SELECT * FROM {{ ref('stg_hl7_pv2') }}
),

deduplicated AS (
    /*
    This CTE deduplicates the PV2 data. We're keeping only the most recent
    record for each MESSAGE_CONTROL_ID, based on the extraction timestamp.
    */
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY MESSAGE_CONTROL_ID 
            ORDER BY _AIRBYTE_EXTRACTED_AT DESC
        ) AS row_num
    FROM stg_pv2
),

aggregated AS (
    /*
    This CTE aggregates the PV2 data, taking the MAX of each field.
    This is done to match the logic in the original script, which used
    MAX() for each field when grouping by MESSAGE_CONTROL_ID.
    */
    SELECT
        MESSAGE_CONTROL_ID,
        MAX(ADMIT_REASON_ID) AS ADMIT_REASON_ID,
        MAX(ADMIT_REASON_TEXT) AS ADMIT_REASON_TEXT,
        MAX(ACTUAL_LENGTH_OF_INPATIENT_STAY) AS ACTUAL_LENGTH_OF_INPATIENT_STAY,
        MAX(ARRV_MODE_CD_SSUKT) AS ARRV_MODE_CD_SSUKT,
        MAX(SOURCE_INTERFACE) AS SOURCE_INTERFACE,
        MAX(ACCOMMODATION_CODE_ID) AS ACCOMMODATION_CODE_ID,
        MAX(PV2_HASH_KEY) AS PV2_HASH_KEY
    FROM deduplicated
    WHERE row_num = 1  -- Only consider the most recent record for each MESSAGE_CONTROL_ID
    GROUP BY MESSAGE_CONTROL_ID
)

/*
The final SELECT statement. We're selecting all fields from the aggregated CTE.
This gives us one row per MESSAGE_CONTROL_ID with the most recent and/or 
maximum values for each field.
*/
SELECT
    MESSAGE_CONTROL_ID,
    ADMIT_REASON_ID,
    ADMIT_REASON_TEXT,
    ACTUAL_LENGTH_OF_INPATIENT_STAY,
    ARRV_MODE_CD_SSUKT,
    SOURCE_INTERFACE,
    ACCOMMODATION_CODE_ID,
    PV2_HASH_KEY
FROM aggregated