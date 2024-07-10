-- models/intermediate/int_point_of_origin_codes.sql

WITH stg_cmn_code AS (
    SELECT * FROM {{ ref('stg_hl7_cmn_code') }}
),

point_of_origin_codes AS (
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
    WHERE SRS_SYS_REF_CD = 'PT_OF_ORIG_CD'
),

ranked_codes AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY CD_SSUKT 
            ORDER BY EFCTV_FR_DT DESC
        ) AS row_num
    FROM point_of_origin_codes
)

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