/*

This model is focusing on creating unique timestamps for each record:

1. Timestamp Uniqueness:
   - Creates a UNIQUE_VLD_FR_TS field by adding microseconds to the original VLD_FR_TS.
   - Uses ROW_NUMBER() to generate a unique microsecond offset for each record within
     its ENCNT_SK and second-level timestamp group.

2. Partitioning Strategy:
   - Partitions data by ENCNT_SK and second-truncated VLD_FR_TS.
   - Orders records within each partition by MSG_CTRL_ID_TXT, REC_RANK, and DW_INSRT_TS.

3. Timestamp Manipulation:
   - Uses TIMESTAMP_TRUNC to truncate VLD_FR_TS to seconds for consistent partitioning.
   - Employs TIMESTAMP_ADD to add the calculated microsecond offset.

4. Data Preservation:
   - Selects all original fields along with the new UNIQUE_VLD_FR_TS.
*/

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