-- models/intermediate/int_discharge_status_codes.sql

WITH stg_cmn_code AS (
    SELECT * FROM {{ ref('stg_hl7_cmn_code') }}
),

discharge_status_codes AS (
    /*
    This CTE filters the common codes table for discharge status codes.
    We're only interested in codes where SRS_SYS_REF_CD is 'DSCRG_STS_CD'.
    */
    SELECT
        CD_SSUKT,
        CD_SK,
        CD,
        CD_DESC,
        CD_ALT_DESC,
        EFCTV_FR_DT,
        EFCTV_TO_DT,
        CMN_CODE_HASH_KEY
    FROM stg_cmn_code
    WHERE SRS_SYS_REF_CD = 'DSCRG_STS_CD'
),

ranked_codes AS (
    /*
    This CTE ranks the discharge status codes by their effective date.
    We use ROW_NUMBER() to assign a rank to each code within its CD_SSUKT group,
    with the most recent effective date getting the lowest rank (1).
    */
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY CD_SSUKT 
            ORDER BY EFCTV_FR_DT DESC
        ) AS row_num
    FROM discharge_status_codes
)

/*
The final SELECT statement chooses only the most recent (effective) 
discharge status code for each unique CD_SSUKT.
*/
SELECT
    CD_SSUKT,
    CD_SK,
    CD,
    CD_DESC,
    CD_ALT_DESC,
    EFCTV_FR_DT,
    EFCTV_TO_DT,
    CMN_CODE_HASH_KEY
FROM ranked_codes
WHERE row_num = 1