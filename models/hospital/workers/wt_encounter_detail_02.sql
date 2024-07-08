-- models/worker_transformations/wt_encounter_detail_02.sql

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