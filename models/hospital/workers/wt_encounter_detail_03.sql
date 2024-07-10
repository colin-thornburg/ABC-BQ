/*

This is focusing on source-specific logic for EPIC and Meditech systems:

1. EPIC-specific Logic:
   - Filters for EPIC encounters with a specific COID ('26960').
   - Ranks EPIC encounters within each ENCNT_SK group.
   - Updates ENCNT_TS, ADMT_TS, DSCRG_TS, and DSCRG_STS_CD_SK for EPIC encounters.

2. Meditech-specific Logic:
   - Filters for Meditech encounters (MEDITECH 6.0 and EXPANSE).
   - Ranks Meditech encounters within each ENCNT_SK group.
   - Updates EXPCT_NUM_OF_INS_PLAN_CNT for Meditech encounters.

3. Data Integration:
   - Joins the main dataset with EPIC and Meditech specific data.
   - Uses COALESCE to prioritize source-specific data over general data.

4. Field Updates:
   - Creates new fields (UPDATED_*) that contain the source-specific values when applicable.

*/

WITH wt_encounter_detail_02 AS (
    SELECT * FROM {{ ref('wt_encounter_detail_02') }}
),

-- CTE for EPIC-specific logic
epic_logic AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ENCNT_SK 
            ORDER BY VLD_FR_TS DESC, MSG_CTRL_ID_TXT DESC, REC_RANK DESC, DW_INSRT_TS DESC
        ) AS EPIC_RANK
    FROM wt_encounter_detail_02
    WHERE SRC_SYS_REF_CD = 'EPIC' AND CAST(COID AS STRING) = '26960'
),

-- CTE for Meditech-specific logic
meditech_logic AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ENCNT_SK 
            ORDER BY VLD_FR_TS DESC, MSG_CTRL_ID_TXT DESC, REC_RANK DESC, DW_INSRT_TS DESC
        ) AS MEDITECH_RANK
    FROM wt_encounter_detail_02
    WHERE SRC_SYS_REF_CD IN ('MEDITECH 6.0', 'EXPANSE')
),

-- Join all together
joined_data AS (
    SELECT
        wd.*,
        e.ENCNT_TS AS EPIC_ENCNT_TS,
        e.ADMT_TS AS EPIC_ADMT_TS,
        e.DSCRG_TS AS EPIC_DSCRG_TS,
        e.DSCRG_STS_CD_SK AS EPIC_DSCRG_STS_CD_SK,
        m.EXPCT_NUM_OF_INS_PLAN_CNT AS MEDITECH_EXPCT_NUM_OF_INS_PLAN_CNT
    FROM wt_encounter_detail_02 wd
    LEFT JOIN epic_logic e ON wd.ENCNT_SK = e.ENCNT_SK AND e.EPIC_RANK = 1
    LEFT JOIN meditech_logic m ON wd.ENCNT_SK = m.ENCNT_SK AND m.MEDITECH_RANK = 1
)

SELECT
    *,
    CASE 
        WHEN SRC_SYS_REF_CD = 'EPIC' AND CAST(COID AS STRING) = '26960' THEN COALESCE(EPIC_ENCNT_TS, ENCNT_TS)
        ELSE ENCNT_TS
    END AS UPDATED_ENCNT_TS,
    CASE 
        WHEN SRC_SYS_REF_CD = 'EPIC' AND CAST(COID AS STRING) = '26960' THEN COALESCE(EPIC_ADMT_TS, ADMT_TS)
        ELSE ADMT_TS
    END AS UPDATED_ADMT_TS,
    CASE 
        WHEN SRC_SYS_REF_CD = 'EPIC' AND CAST(COID AS STRING) = '26960' THEN COALESCE(EPIC_DSCRG_TS, DSCRG_TS)
        ELSE DSCRG_TS
    END AS UPDATED_DSCRG_TS,
    CASE 
        WHEN SRC_SYS_REF_CD = 'EPIC' AND CAST(COID AS STRING) = '26960' THEN COALESCE(EPIC_DSCRG_STS_CD_SK, DSCRG_STS_CD_SK)
        ELSE DSCRG_STS_CD_SK
    END AS UPDATED_DSCRG_STS_CD_SK,
    CASE 
        WHEN SRC_SYS_REF_CD IN ('MEDITECH 6.0', 'EXPANSE') THEN COALESCE(MEDITECH_EXPCT_NUM_OF_INS_PLAN_CNT, EXPCT_NUM_OF_INS_PLAN_CNT)
        ELSE EXPCT_NUM_OF_INS_PLAN_CNT
    END AS UPDATED_EXPCT_NUM_OF_INS_PLAN_CNT
FROM joined_data