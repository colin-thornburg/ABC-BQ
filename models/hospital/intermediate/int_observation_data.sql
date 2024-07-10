-- models/intermediate/int_observation_data.sql

WITH stg_obx AS (
    SELECT * FROM {{ ref('stg_hl7_obx') }}
),

filtered_obx AS (
    /*
    This CTE filters the OBX data for specific conditions.
    We're only interested in observations related to 'ENCNTR_ID' for Cerner
    and 'PRICSN' for Epic, as per the original script.
    */
    SELECT *
    FROM stg_obx
    WHERE (SOURCE_INTERFACE = 'Cerner' AND OBSV_ID_ID = 'ENCNTR_ID')
       OR (SOURCE_INTERFACE = 'Epic' AND VALUE_TYPE = 'NM' AND OBSV_ID_ID = 'PRICSN')
),

ranked_obx AS (
    /*
    This CTE ranks the OBX records for each MESSAGE_CONTROL_ID.
    We use ROW_NUMBER() to assign a rank based on _AIRBYTE_EXTRACTED_AT in descending order.
    This allows us to identify the most recent observation record for each message.
    */
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY MESSAGE_CONTROL_ID 
            ORDER BY _AIRBYTE_EXTRACTED_AT DESC
        ) AS row_num
    FROM filtered_obx
),

latest_obx AS (
    /*
    This CTE selects only the most recent observation record for each MESSAGE_CONTROL_ID.
    We're using row_num = 1 to get the most recent record.
    */
    SELECT *
    FROM ranked_obx
    WHERE row_num = 1
)

/*
The final SELECT statement. We're selecting relevant fields from the latest_obx CTE.
We're using CASE statements to handle different logic for Cerner and Epic interfaces.
*/
SELECT
    MESSAGE_CONTROL_ID,
    SOURCE_INTERFACE,
    CASE 
        WHEN SOURCE_INTERFACE = 'Cerner' THEN OBSV_VALUE_TEXT
        WHEN SOURCE_INTERFACE = 'Epic' THEN CAST(OBSV_VALUE_NUMERIC AS STRING)
        ELSE NULL
    END AS ENCOUNTER_ID,
    OBX_HASH_KEY
FROM latest_obx