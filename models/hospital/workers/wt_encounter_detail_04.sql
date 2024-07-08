-- models/worker_transformations/wt_encounter_detail_04.sql

WITH wt_encounter_detail_03 AS (
    SELECT * FROM {{ ref('wt_encounter_detail_03') }}
),

unique_timestamps AS (
    SELECT 
        *,
        -- Create a unique timestamp by adding microseconds based on the row number
        TIMESTAMP_ADD(
            TIMESTAMP_TRUNC(VLD_FR_TS, SECOND),
            INTERVAL (ROW_NUMBER() OVER(
                PARTITION BY ENCNT_SK, 
                TIMESTAMP_TRUNC(VLD_FR_TS, SECOND)  -- Truncate to seconds for partitioning
                ORDER BY MSG_CTRL_ID_TXT, REC_RANK, DW_INSRT_TS
            ) - 1) MICROSECOND
        ) AS UNIQUE_VLD_FR_TS
    FROM wt_encounter_detail_03
)

SELECT
    *
FROM unique_timestamps