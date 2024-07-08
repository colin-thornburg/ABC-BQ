-- models/intermediate/int_patient_info.sql

WITH stg_pid AS (
    SELECT * FROM {{ ref('stg_hl7_pid') }}
),

ranked_pid AS (
    /*
    This CTE ranks the PID records for each MESSAGE_CONTROL_ID.
    We use ROW_NUMBER() to assign a rank based on _AIRBYTE_EXTRACTED_AT in descending order.
    This allows us to identify the most recent patient record for each message.
    */
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY MESSAGE_CONTROL_ID 
            ORDER BY _AIRBYTE_EXTRACTED_AT DESC
        ) AS row_num
    FROM stg_pid
),

latest_pid AS (
    /*
    This CTE selects only the most recent patient record for each MESSAGE_CONTROL_ID.
    We're using row_num = 1 to get the most recent record.
    */
    SELECT *
    FROM ranked_pid
    WHERE row_num = 1
)

/*
The final SELECT statement. We're selecting relevant fields from the latest_pid CTE.
We're using CASE statements to handle different logic for different source interfaces.
*/
SELECT
    MESSAGE_CONTROL_ID,
    PATIENT_DEATH_DATE_AND_TIME,
    PATIENT_DEATH_INDICATOR,
    CASE 
        WHEN SOURCE_INTERFACE = 'Cerner' THEN ALTERNATE_PATIENT_ID_ID_NUM
        ELSE PATIENT_ID_ALTERNATE_ID
    END AS ALTERNATE_PATIENT_ID,
    PATIENT_ID_LIST_ID_TYPE_CODE,
    PATIENT_ID_LIST_ID_NUM,
    SOURCE_INTERFACE,
    PID_HASH_KEY
FROM latest_pid