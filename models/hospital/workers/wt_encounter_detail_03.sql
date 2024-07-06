-- models/worker_transformations/wt_encounter_detail_03.sql

WITH wt_encounter_detail_02 AS (
    SELECT * FROM {{ ref('wt_encounter_detail_02') }}
),

-- CTE for EPIC-specific logic
epic_logic AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ENCNT_SK 
            ORDER BY VLD_FR_TS, MSG_CTRL_ID_TXT, REC_RANK, DW_INSRT_TS
        ) AS EPIC_RANK
    FROM wt_encounter_detail_02
    WHERE SRC_SYS_REF_CD = 'EPIC' AND COID = '26960'
),

-- CTE for Meditech-specific logic
meditech_logic AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ENCNT_SK 
            ORDER BY VLD_FR_TS, MSG_CTRL_ID_TXT, REC_RANK, DW_INSRT_TS
        ) AS MEDITECH_RANK
    FROM wt_encounter_detail_02
    WHERE SRC_SYS_REF_CD IN ('MEDITECH 6.0', 'EXPANSE')
)

SELECT
    wd.*,
    -- EPIC-specific ENCNT_TS and ADMT_TS logic
    CASE 
        WHEN wd.SRC_SYS_REF_CD = 'EPIC' AND wd.COID = '26960' 
        THEN COALESCE(
            (SELECT e.ENCNT_TS 
             FROM epic_logic e 
             WHERE e.ENCNT_SK = wd.ENCNT_SK AND e.EPIC_RANK <= wd.REC_RANK
             ORDER BY e.EPIC_RANK DESC
             LIMIT 1),
            wd.ENCNT_TS
        )
        ELSE wd.ENCNT_TS
    END AS ENCNT_TS,
    CASE 
        WHEN wd.SRC_SYS_REF_CD = 'EPIC' AND wd.COID = '26960' 
        THEN COALESCE(
            (SELECT e.ADMT_TS 
             FROM epic_logic e 
             WHERE e.ENCNT_SK = wd.ENCNT_SK AND e.EPIC_RANK <= wd.REC_RANK
             ORDER BY e.EPIC_RANK DESC
             LIMIT 1),
            wd.ADMT_TS
        )
        ELSE wd.ADMT_TS
    END AS ADMT_TS,
    -- EPIC-specific DSCRG_TS and DSCRG_STS_CD_SK logic
    CASE 
        WHEN wd.SRC_SYS_REF_CD = 'EPIC' AND wd.COID = '26960' 
        THEN COALESCE(
            (SELECT e.DSCRG_TS 
             FROM epic_logic e 
             WHERE e.ENCNT_SK = wd.ENCNT_SK AND e.EPIC_RANK <= wd.REC_RANK
             ORDER BY e.EPIC_RANK DESC
             LIMIT 1),
            wd.DSCRG_TS
        )
        ELSE wd.DSCRG_TS
    END AS DSCRG_TS,
    CASE 
        WHEN wd.SRC_SYS_REF_CD = 'EPIC' AND wd.COID = '26960' 
        THEN COALESCE(
            (SELECT e.DSCRG_STS_CD_SK 
             FROM epic_logic e 
             WHERE e.ENCNT_SK = wd.ENCNT_SK AND e.EPIC_RANK <= wd.REC_RANK
             ORDER BY e.EPIC_RANK DESC
             LIMIT 1),
            wd.DSCRG_STS_CD_SK
        )
        ELSE wd.DSCRG_STS_CD_SK
    END AS DSCRG_STS_CD_SK,
    -- Meditech-specific EXPCT_NUM_OF_INS_PLAN_CNT logic
    CASE 
        WHEN wd.SRC_SYS_REF_CD IN ('MEDITECH 6.0', 'EXPANSE') 
        THEN COALESCE(
            (SELECT m.EXPCT_NUM_OF_INS_PLAN_CNT 
             FROM meditech_logic m 
             WHERE m.ENCNT_SK = wd.ENCNT_SK AND m.MEDITECH_RANK <= wd.REC_RANK
             ORDER BY m.MEDITECH_RANK DESC
             LIMIT 1),
            wd.EXPCT_NUM_OF_INS_PLAN_CNT
        )
        ELSE wd.EXPCT_NUM_OF_INS_PLAN_CNT
    END AS EXPCT_NUM_OF_INS_PLAN_CNT
FROM wt_encounter_detail_02 wd