-- models/intermediate/int_insurance_info.sql

WITH stg_in1 AS (
    SELECT * FROM {{ ref('stg_hl7_in1') }}
),

ranked_insurance AS (
    /*
    This CTE ranks the insurance records for each MESSAGE_CONTROL_ID.
    We use ROW_NUMBER() to assign a rank based on SET_ID in descending order.
    This allows us to identify the most recent insurance record for each message.
    */
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY MESSAGE_CONTROL_ID 
            ORDER BY SET_ID DESC, _AIRBYTE_EXTRACTED_AT DESC
        ) AS row_num
    FROM stg_in1
),

latest_insurance AS (
    /*
    This CTE selects only the most recent insurance record for each MESSAGE_CONTROL_ID.
    We're using row_num = 1 to get the record with the highest SET_ID.
    */
    SELECT *
    FROM ranked_insurance
    WHERE row_num = 1
),

insurance_count AS (
    /*
    This CTE counts the number of insurance records for each MESSAGE_CONTROL_ID.
    This count represents the expected number of insurance plans.
    */
    SELECT
        MESSAGE_CONTROL_ID,
        COUNT(*) AS EXPCT_NUM_OF_INS_PLAN_CNT
    FROM stg_in1
    GROUP BY MESSAGE_CONTROL_ID
)

/*
The final SELECT statement combines the latest insurance information
with the count of insurance plans for each MESSAGE_CONTROL_ID.
*/
SELECT
    l.MESSAGE_CONTROL_ID,
    l.SET_ID,
    l.INSURANCE_PLAN_ID,
    l.INSURANCE_COMPANY_ID,
    l.INSURANCE_COMPANY_NAME,
    l.INSURANCE_COMPANY_ADDRESS,
    l.INSURANCE_CO_CONTACT_PERSON,
    l.INSURANCE_CO_PHONE_NUMBER,
    l.GROUP_NAME,
    l.GROUP_NUMBER,
    l.PLAN_EFFECTIVE_DATE,
    l.PLAN_EXPIRATION_DATE,
    l.POLICY_NUMBER,
    l.POLICY_TYPE,
    l.IN1_HASH_KEY,
    c.EXPCT_NUM_OF_INS_PLAN_CNT
FROM latest_insurance l
JOIN insurance_count c ON l.MESSAGE_CONTROL_ID = c.MESSAGE_CONTROL_ID