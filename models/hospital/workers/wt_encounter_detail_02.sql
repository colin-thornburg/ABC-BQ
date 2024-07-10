/*

1. Assigns a rank (REC_RANK) to each record within an ENCNT_SK group, 
   ordered by VLD_FR_TS, MSG_CTRL_ID_TXT, and DW_INSRT_TS.
   This ranking seems to help with future CDC operations.

2. Adds two new fields:
   - MESSAGE_TYPE_TRIGGER_EVENT_DIS: Set to 'A13' if DSCRG_TS is NULL, otherwise 'A03'.
     This helps distinguish between ongoing and completed encounters.
   - MESSAGE_TYPE_TRIGGER_EVENT_INS: Set to 'A03' if the original MESSAGE_TYPE_TRIGGER_EVENT
     is 'A03', otherwise 'A01'. This aids in identifying admission events.

These derived fields and rankings prepare the data for handling
different types of admission and discharge events.
*/

WITH wt_encounter_detail_01 AS (
    SELECT * FROM {{ ref('wt_encounter_detail_01') }}
),

ranked_encounters AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY ENCNT_SK 
            ORDER BY VLD_FR_TS, MSG_CTRL_ID_TXT, DW_INSRT_TS
        ) AS REC_RANK
    FROM wt_encounter_detail_01
)

SELECT
    ranked_encounters.*,  -- This includes DW_INSRT_TS
    -- Add fields for discharge and admission message types
    CASE 
        WHEN DSCRG_TS IS NULL THEN 'A13' 
        ELSE 'A03' 
    END AS MESSAGE_TYPE_TRIGGER_EVENT_DIS,
    CASE 
        WHEN TRIM(MESSAGE_TYPE_TRIGGER_EVENT) = 'A03' THEN MESSAGE_TYPE_TRIGGER_EVENT 
        ELSE 'A01' 
    END AS MESSAGE_TYPE_TRIGGER_EVENT_INS
FROM ranked_encounters