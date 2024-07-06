
/***** 
We've added a ranking step using ROW_NUMBER(), partitioning by CD_SSUKT and ordering by EFCTV_FR_DT DESC. 
This ensures we're only keeping the most recent (effective) code for each CD_SSUKT.
The final SELECT statement chooses only the most recent record for each CD_SSUKT.
*****/
WITH stg_cmn_code AS (
    SELECT * FROM {{ ref('stg_hl7_cmn_code') }}
),

visit_type_codes AS (
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
    WHERE SRS_SYS_REF_CD = 'VST_TYPE_CD'
),

ranked_codes AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY CD_SSUKT 
            ORDER BY EFCTV_FR_DT DESC
        ) AS row_num
    FROM visit_type_codes
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