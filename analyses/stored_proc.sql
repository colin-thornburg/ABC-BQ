CREATE OR REPLACE PROCEDURE `sales-demo-project-314714.dbt_hca_cthornburg.sp_process_encounter_detail`()
BEGIN
  

  -- Create temporary table for step 01
  CREATE TEMP TABLE IF NOT EXISTS `${temp_table_prefix}01` AS (
    WITH int_encounter_detail AS (
      SELECT * FROM `sales-demo-project-314714.dbt_hca_cthornburg.int_encounter_detail`
    )
    SELECT
      ENCNT_SK,
      VLD_FR_TS,
      PATIENT_DW_ID,
      PAT_ACCT_NUM,
      COMPANY_CODE,
      COID,
      VLD_TO_TS,
      CASE 
        WHEN SOURCE_INTERFACE = 'EPIC' AND TRIM(MESSAGE_TYPE_TRIGGER_EVENT) = 'A11' THEN NULL 
        ELSE ADMT_TS
      END AS ENCNT_TS,
      ALT_ENCNT_TXT,
      VST_TYPE_CD_SK,
      EHR_MED_REC_NUM,
      EHR_PAT_ACCT_NUM,
      EHR_MED_URN,
      PT_OF_ORIG_CD_SK,
      ADMT_TYPE_REF_CD,
      CASE 
        WHEN SOURCE_INTERFACE = 'EPIC' AND TRIM(MESSAGE_TYPE_TRIGGER_EVENT) = 'A11' THEN NULL 
        ELSE ADMT_TS
      END AS ADMT_TS,
      ADMT_CMPLN_TXT,
      RE_ADMT_IND,
      HSPTL_SRVC_CD_SK,
      EXPCT_NUM_OF_INS_PLAN_CNT,
      ACTL_LEN_OF_STAY_DYS_CNT,
      EMPMNT_ILLNS_IND,
      ACDNT_AUTO_ST_CD,
      ACDNT_TYPE_REF_CD,
      ACDNT_TS,
      CASE 
        WHEN SOURCE_INTERFACE = 'EPIC' AND TRIM(MESSAGE_TYPE_TRIGGER_EVENT) IN ('A13','A01') THEN NULL 
        ELSE DSCRG_TS
      END AS DSCRG_TS,
      CASE 
        WHEN SOURCE_INTERFACE = 'EPIC' AND TRIM(MESSAGE_TYPE_TRIGGER_EVENT) IN ('A13','A01') THEN 0 
        ELSE DSCRG_STS_CD_SK
      END AS DSCRG_STS_CD_SK,
      PTNT_DTH_TS,
      PTNT_DTH_IND,
      PTNT_STS_REF_CD,
      PTNT_CLASS_REF_CD,
      DATE(VLD_FR_TS) AS PTNT_STS_EFFV_DT,
      EMR_PTNT_ID_ASSGN_AUTH,
      ADMT_TYPE_REF_CD AS PRTY_REF_CD,
      SIG_DT,
      ADV_DRTCV_PRSNT_TXT,
      EFFV_FR_DT,
      EFFV_TO_DT,
      TYPE_REF_CD,
      ACCOM_REF_CD,
      SOURCE_INTERFACE AS SRC_SYS_REF_CD,
      SRC_SYS_UNQ_KEY_TXT,
      CRT_RUN_ID,
      LST_UPDT_RUN_ID,
      MESSAGE_CONTROL_ID AS MSG_CTRL_ID_TXT,
      MESSAGE_TYPE_TRIGGER_EVENT,
      DW_INSRT_TS
    FROM int_encounter_detail
    WHERE ENCNT_SK IS NOT NULL 
  );

  -- Create temporary table for step 02
  CREATE TEMP TABLE IF NOT EXISTS `temp_encounter_detail_02` AS (
    WITH ranked_encounters AS (
      SELECT 
        *,
        ROW_NUMBER() OVER(
          PARTITION BY ENCNT_SK 
          ORDER BY VLD_FR_TS, MSG_CTRL_ID_TXT, DW_INSRT_TS
        ) AS REC_RANK
      FROM `${temp_table_prefix}01`
    )
    SELECT
      ranked_encounters.*,
      CASE 
        WHEN DSCRG_TS IS NULL THEN 'A13' 
        ELSE 'A03' 
      END AS MESSAGE_TYPE_TRIGGER_EVENT_DIS,
      CASE 
        WHEN TRIM(MESSAGE_TYPE_TRIGGER_EVENT) = 'A03' THEN MESSAGE_TYPE_TRIGGER_EVENT 
        ELSE 'A01' 
      END AS MESSAGE_TYPE_TRIGGER_EVENT_INS
    FROM ranked_encounters);


  -- Create temporary table for step 03
  CREATE TEMP TABLE IF NOT EXISTS `${temp_table_prefix}03` AS (
    WITH epic_logic AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY ENCNT_SK 
          ORDER BY VLD_FR_TS DESC, MSG_CTRL_ID_TXT DESC, REC_RANK DESC, DW_INSRT_TS DESC
        ) AS EPIC_RANK
      FROM `${temp_table_prefix}02`
      WHERE SRC_SYS_REF_CD = 'EPIC' AND CAST(COID AS STRING) = '26960'
    ),
    meditech_logic AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY ENCNT_SK 
          ORDER BY VLD_FR_TS DESC, MSG_CTRL_ID_TXT DESC, REC_RANK DESC, DW_INSRT_TS DESC
        ) AS MEDITECH_RANK
      FROM `${temp_table_prefix}02`
      WHERE SRC_SYS_REF_CD IN ('MEDITECH 6.0', 'EXPANSE')
    ),
    joined_data AS (
      SELECT
        wd.*,
        e.ENCNT_TS AS EPIC_ENCNT_TS,
        e.ADMT_TS AS EPIC_ADMT_TS,
        e.DSCRG_TS AS EPIC_DSCRG_TS,
        e.DSCRG_STS_CD_SK AS EPIC_DSCRG_STS_CD_SK,
        m.EXPCT_NUM_OF_INS_PLAN_CNT AS MEDITECH_EXPCT_NUM_OF_INS_PLAN_CNT
      FROM `${temp_table_prefix}02` wd
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
  );

  -- Create temporary table for step 04
  CREATE TEMP TABLE IF NOT EXISTS `${temp_table_prefix}04` AS (
    WITH unique_timestamps AS (
      SELECT 
        *,
        TIMESTAMP_ADD(
          TIMESTAMP_TRUNC(VLD_FR_TS, SECOND),
          INTERVAL (ROW_NUMBER() OVER(
            PARTITION BY ENCNT_SK, 
            TIMESTAMP_TRUNC(VLD_FR_TS, SECOND)
            ORDER BY MSG_CTRL_ID_TXT, REC_RANK, DW_INSRT_TS
          ) - 1) MICROSECOND
        ) AS UNIQUE_VLD_FR_TS
      FROM `${temp_table_prefix}03`
    )
    SELECT *
    FROM unique_timestamps
  );

  -- Create temporary table for step 05
  CREATE TEMP TABLE IF NOT EXISTS `${temp_table_prefix}05` AS (
    WITH ranked_records AS (
      SELECT 
        *,
        ROW_NUMBER() OVER(
          PARTITION BY ENCNT_SK 
          ORDER BY VLD_FR_TS, MSG_CTRL_ID_TXT, DW_INSRT_TS
        ) AS REC_RANK
      FROM `${temp_table_prefix}04`
    ),
    last_non_null_accom AS (
      SELECT
        ranked_records.*,
        LAST_VALUE(ACCOM_REF_CD IGNORE NULLS) OVER (
          PARTITION BY ENCNT_SK 
          ORDER BY VLD_FR_TS 
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS LAST_NON_NULL_ACCOM_REF_CD
      FROM ranked_records
    )
    SELECT
      last_non_null_accom.REC_RANK,
      last_non_null_accom.ENCNT_SK,
      last_non_null_accom.VLD_FR_TS,
      last_non_null_accom.PATIENT_DW_ID,
      last_non_null_accom.PAT_ACCT_NUM,
      last_non_null_accom.COMPANY_CODE,
      last_non_null_accom.COID,
      last_non_null_accom.VLD_TO_TS,
      last_non_null_accom.ENCNT_TS,
      last_non_null_accom.ALT_ENCNT_TXT,
      last_non_null_accom.VST_TYPE_CD_SK,
      last_non_null_accom.EHR_MED_REC_NUM,
      last_non_null_accom.EHR_PAT_ACCT_NUM,
      last_non_null_accom.EHR_MED_URN,
      last_non_null_accom.PT_OF_ORIG_CD_SK,
      last_non_null_accom.ADMT_TYPE_REF_CD,
      last_non_null_accom.ADMT_TS,
      last_non_null_accom.ADMT_CMPLN_TXT,
      last_non_null_accom.RE_ADMT_IND,
      last_non_null_accom.HSPTL_SRVC_CD_SK,
      last_non_null_accom.EXPCT_NUM_OF_INS_PLAN_CNT,
      last_non_null_accom.ACTL_LEN_OF_STAY_DYS_CNT,
      last_non_null_accom.EMPMNT_ILLNS_IND,
      last_non_null_accom.ACDNT_AUTO_ST_CD,
      last_non_null_accom.ACDNT_TYPE_REF_CD,
      last_non_null_accom.ACDNT_TS,
      last_non_null_accom.DSCRG_TS,
      last_non_null_accom.DSCRG_STS_CD_SK,
      last_non_null_accom.PTNT_DTH_TS,
      last_non_null_accom.PTNT_DTH_IND,
      last_non_null_accom.PTNT_STS_REF_CD,
      last_non_null_accom.PTNT_CLASS_REF_CD,
      last_non_null_accom.PTNT_STS_EFFV_DT,
      last_non_null_accom.EMR_PTNT_ID_ASSGN_AUTH,
      last_non_null_accom.PRTY_REF_CD,
      last_non_null_accom.SIG_DT,
      last_non_null_accom.ADV_DRTCV_PRSNT_TXT,
      last_non_null_accom.EFFV_FR_DT,
      last_non_null_accom.EFFV_TO_DT,
      last_non_null_accom.TYPE_REF_CD,
      CASE 
        WHEN last_non_null_accom.SRC_SYS_REF_CD = 'EPIC' THEN LAST_NON_NULL_ACCOM_REF_CD
        ELSE last_non_null_accom.ACCOM_REF_CD 
      END AS ACCOM_REF_CD,
      last_non_null_accom.SRC_SYS_REF_CD,
      last_non_null_accom.SRC_SYS_UNQ_KEY_TXT,
      last_non_null_accom.CRT_RUN_ID,
      last_non_null_accom.LST_UPDT_RUN_ID,
      last_non_null_accom.MSG_CTRL_ID_TXT,
      last_non_null_accom.MESSAGE_TYPE_TRIGGER_EVENT,
      last_non_null_accom.MESSAGE_TYPE_TRIGGER_EVENT_DIS,
      last_non_null_accom.MESSAGE_TYPE_TRIGGER_EVENT_INS,
      last_non_null_accom.DW_INSRT_TS
    FROM last_non_null_accom
  );
-- Create temporary table for step 06
  CREATE TEMP TABLE IF NOT EXISTS `${temp_table_prefix}06` AS (
    WITH prev_record AS (
      SELECT 
        *,
        LAG(VST_TYPE_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_VST_TYPE_CD_SK,
        LAG(PT_OF_ORIG_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_PT_OF_ORIG_CD_SK,
        LAG(ENCNT_TS) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ENCNT_TS,
        LAG(ADMT_TS) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ADMT_TS,
        LAG(ADMT_CMPLN_TXT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ADMT_CMPLN_TXT,
        LAG(RE_ADMT_IND) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_RE_ADMT_IND,
        LAG(HSPTL_SRVC_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_HSPTL_SRVC_CD_SK,
        LAG(EXPCT_NUM_OF_INS_PLAN_CNT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_EXPCT_NUM_OF_INS_PLAN_CNT,
        LAG(ACTL_LEN_OF_STAY_DYS_CNT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ACTL_LEN_OF_STAY_DYS_CNT,
        LAG(DSCRG_TS) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_DSCRG_TS,
        LAG(DSCRG_STS_CD_SK) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_DSCRG_STS_CD_SK,
        LAG(PTNT_STS_REF_CD) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_PTNT_STS_REF_CD,
        LAG(ADV_DRTCV_PRSNT_TXT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ADV_DRTCV_PRSNT_TXT,
        LAG(ACCOM_REF_CD) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ACCOM_REF_CD,
        LAG(ALT_ENCNT_TXT) OVER (PARTITION BY ENCNT_SK ORDER BY REC_RANK) AS PREV_ALT_ENCNT_TXT
      FROM `${temp_table_prefix}05`
    )
    SELECT 
      *,
      CASE
        WHEN REC_RANK = 1 THEN 1
        WHEN COALESCE(CAST(VST_TYPE_CD_SK AS STRING), 'NULL') != COALESCE(CAST(PREV_VST_TYPE_CD_SK AS STRING), 'NULL') THEN 1
        WHEN COALESCE(CAST(PT_OF_ORIG_CD_SK AS STRING), 'NULL') != COALESCE(CAST(PREV_PT_OF_ORIG_CD_SK AS STRING), 'NULL') THEN 1
        WHEN COALESCE(CAST(ENCNT_TS AS STRING), '1900-01-01 00:00:00') != COALESCE(CAST(PREV_ENCNT_TS AS STRING), '1900-01-01 00:00:00') THEN 1
        WHEN COALESCE(CAST(ADMT_TS AS STRING), '1900-01-01 00:00:00') != COALESCE(CAST(PREV_ADMT_TS AS STRING), '1900-01-01 00:00:00') THEN 1
        WHEN COALESCE(ADMT_CMPLN_TXT, '') != COALESCE(PREV_ADMT_CMPLN_TXT, '') THEN 1
        WHEN COALESCE(CAST(RE_ADMT_IND AS STRING), 'NULL') != COALESCE(CAST(PREV_RE_ADMT_IND AS STRING), 'NULL') THEN 1
        WHEN COALESCE(CAST(HSPTL_SRVC_CD_SK AS STRING), 'NULL') != COALESCE(CAST(PREV_HSPTL_SRVC_CD_SK AS STRING), 'NULL') THEN 1
        WHEN COALESCE(CAST(EXPCT_NUM_OF_INS_PLAN_CNT AS STRING), 'NULL') != COALESCE(CAST(PREV_EXPCT_NUM_OF_INS_PLAN_CNT AS STRING), 'NULL') THEN 1
        WHEN COALESCE(CAST(ACTL_LEN_OF_STAY_DYS_CNT AS STRING), 'NULL') != COALESCE(CAST(PREV_ACTL_LEN_OF_STAY_DYS_CNT AS STRING), 'NULL') THEN 1
        WHEN COALESCE(CAST(DSCRG_TS AS STRING), '1900-01-01 00:00:00') != COALESCE(CAST(PREV_DSCRG_TS AS STRING), '1900-01-01 00:00:00') THEN 1
        WHEN COALESCE(CAST(DSCRG_STS_CD_SK AS STRING), 'NULL') != COALESCE(CAST(PREV_DSCRG_STS_CD_SK AS STRING), 'NULL') THEN 1
        WHEN COALESCE(PTNT_STS_REF_CD, '') != COALESCE(PREV_PTNT_STS_REF_CD, '') THEN 1
        WHEN COALESCE(CAST(ADV_DRTCV_PRSNT_TXT AS STRING), 'NULL') != COALESCE(CAST(PREV_ADV_DRTCV_PRSNT_TXT AS STRING), 'NULL') THEN 1
        WHEN COALESCE(ACCOM_REF_CD, '') != COALESCE(PREV_ACCOM_REF_CD, '') THEN 1
        WHEN COALESCE(ALT_ENCNT_TXT, '') != COALESCE(PREV_ALT_ENCNT_TXT, '') THEN 1
        ELSE 0
      END AS IS_CHANGED
    FROM prev_record
  );

  -- Create temporary table for step 07
  CREATE TEMP TABLE IF NOT EXISTS `${temp_table_prefix}07` AS (
    WITH dense_ranked AS (
      SELECT 
        *,
        DENSE_RANK() OVER (
          PARTITION BY ENCNT_SK 
          ORDER BY VLD_FR_TS
        ) AS DENSE_REC_RANK
      FROM `${temp_table_prefix}06`
      WHERE IS_CHANGED = 1
    )
    SELECT *
    FROM dense_ranked
  );

  -- Create temporary table for step 08
  CREATE TEMP TABLE IF NOT EXISTS `${temp_table_prefix}08` AS (
    WITH next_valid_from AS (
      SELECT 
        *,
        LEAD(VLD_FR_TS) OVER (
          PARTITION BY ENCNT_SK 
          ORDER BY DENSE_REC_RANK
        ) AS NEXT_VLD_FR_TS
      FROM `${temp_table_prefix}07`
    )
    SELECT
      ENCNT_SK,
      VLD_FR_TS,
      PATIENT_DW_ID,
      PAT_ACCT_NUM,
      COMPANY_CODE,
      COID,
      COALESCE(
        DATETIME(TIMESTAMP_SUB(NEXT_VLD_FR_TS, INTERVAL 1 MICROSECOND)), 
        DATETIME '9999-12-31 23:59:59.999999'
      ) AS VLD_TO_TS,
      ENCNT_TS,
      ALT_ENCNT_TXT,
      VST_TYPE_CD_SK,
      EHR_MED_REC_NUM,
      EHR_PAT_ACCT_NUM,
      EHR_MED_URN,
      PT_OF_ORIG_CD_SK,
      ADMT_TYPE_REF_CD,
      ADMT_TS,
      ADMT_CMPLN_TXT,
      RE_ADMT_IND,
      HSPTL_SRVC_CD_SK,
      EXPCT_NUM_OF_INS_PLAN_CNT,
      ACTL_LEN_OF_STAY_DYS_CNT,
      EMPMNT_ILLNS_IND,
      ACDNT_AUTO_ST_CD,
      ACDNT_TYPE_REF_CD,
      ACDNT_TS,
      DSCRG_TS,
      DSCRG_STS_CD_SK,
      PTNT_DTH_TS,
      PTNT_DTH_IND,
      PTNT_STS_REF_CD,
      PTNT_CLASS_REF_CD,
      PTNT_STS_EFFV_DT,
      EMR_PTNT_ID_ASSGN_AUTH,
      PRTY_REF_CD,
      SIG_DT,
      ADV_DRTCV_PRSNT_TXT,
      EFFV_FR_DT,
      EFFV_TO_DT,
      TYPE_REF_CD,
      ACCOM_REF_CD,
      SRC_SYS_REF_CD,
      SRC_SYS_UNQ_KEY_TXT,
      CRT_RUN_ID,
      LST_UPDT_RUN_ID,
      MSG_CTRL_ID_TXT,
      MESSAGE_TYPE_TRIGGER_EVENT,
      MESSAGE_TYPE_TRIGGER_EVENT_DIS,
      MESSAGE_TYPE_TRIGGER_EVENT_INS,
      DW_INSRT_TS
    FROM next_valid_from
  );

  -- Create temporary table for step 09
  CREATE TEMP TABLE IF NOT EXISTS `${temp_table_prefix}09` AS (
    SELECT
      ENCNT_SK,
      VLD_FR_TS,
      PATIENT_DW_ID,
      PAT_ACCT_NUM,
      COMPANY_CODE,
      COID,
      CAST(VLD_TO_TS AS TIMESTAMP) AS VLD_TO_TS,
      ENCNT_TS,
      ALT_ENCNT_TXT,
      VST_TYPE_CD_SK,
      EHR_MED_REC_NUM,
      EHR_PAT_ACCT_NUM,
      EHR_MED_URN,
      PT_OF_ORIG_CD_SK,
      ADMT_TYPE_REF_CD,
      ADMT_TS,
      ADMT_CMPLN_TXT,
      RE_ADMT_IND,
      HSPTL_SRVC_CD_SK,
      EXPCT_NUM_OF_INS_PLAN_CNT,
      ACTL_LEN_OF_STAY_DYS_CNT,
      EMPMNT_ILLNS_IND,
      ACDNT_AUTO_ST_CD,
      ACDNT_TYPE_REF_CD,
      ACDNT_TS,
      DSCRG_TS,
      DSCRG_STS_CD_SK,
      PTNT_DTH_TS,
      PTNT_DTH_IND,
      PTNT_STS_REF_CD,
      PTNT_CLASS_REF_CD,
      PTNT_STS_EFFV_DT,
      EMR_PTNT_ID_ASSGN_AUTH,
      PRTY_REF_CD,
      SIG_DT,
      ADV_DRTCV_PRSNT_TXT,
      EFFV_FR_DT,
      EFFV_TO_DT,
      TYPE_REF_CD,
      ACCOM_REF_CD,
      SRC_SYS_REF_CD,
      SRC_SYS_UNQ_KEY_TXT,
      CRT_RUN_ID,
      LST_UPDT_RUN_ID,
      MSG_CTRL_ID_TXT,
      MESSAGE_TYPE_TRIGGER_EVENT AS HL7_EV_ID,
      DW_INSRT_TS
    FROM `${temp_table_prefix}08`
  );

  -- Create the final mart table
  CREATE OR REPLACE TABLE `${final_table}` AS (
    SELECT
      ENCNT_SK,
      VLD_FR_TS,
      PATIENT_DW_ID,
      PAT_ACCT_NUM,
      COMPANY_CODE,
      COID,
      VLD_TO_TS,
      ENCNT_TS,
      ALT_ENCNT_TXT,
      VST_TYPE_CD_SK,
      EHR_MED_REC_NUM,
      EHR_PAT_ACCT_NUM,
      EHR_MED_URN,
      PT_OF_ORIG_CD_SK,
      ADMT_TYPE_REF_CD,
      ADMT_TS,
      ADMT_CMPLN_TXT,
      RE_ADMT_IND,
      HSPTL_SRVC_CD_SK,
      EXPCT_NUM_OF_INS_PLAN_CNT,
      ACTL_LEN_OF_STAY_DYS_CNT,
      EMPMNT_ILLNS_IND,
      ACDNT_AUTO_ST_CD,
      ACDNT_TYPE_REF_CD,
      ACDNT_TS,
      DSCRG_TS,
      DSCRG_STS_CD_SK,
      PTNT_DTH_TS,
      PTNT_DTH_IND,
      PTNT_STS_REF_CD,
      PTNT_CLASS_REF_CD,
      PTNT_STS_EFFV_DT,
      EMR_PTNT_ID_ASSGN_AUTH,
      PRTY_REF_CD,
      SIG_DT,
      ADV_DRTCV_PRSNT_TXT,
      EFFV_FR_DT,
      EFFV_TO_DT,
      TYPE_REF_CD,
      ACCOM_REF_CD,
      SRC_SYS_REF_CD,
      SRC_SYS_UNQ_KEY_TXT,
      CRT_RUN_ID,
      LST_UPDT_RUN_ID,
      MSG_CTRL_ID_TXT,
      HL7_EV_ID,
      DW_INSRT_TS
    FROM `${temp_table_prefix}09`
  );

  -- Clean up temporary tables
  DROP TABLE IF EXISTS `${temp_table_prefix}01`;
  DROP TABLE IF EXISTS `${temp_table_prefix}02`;
  DROP TABLE IF EXISTS `${temp_table_prefix}03`;
  DROP TABLE IF EXISTS `${temp_table_prefix}04`;
  DROP TABLE IF EXISTS `${temp_table_prefix}05`;
  DROP TABLE IF EXISTS `${temp_table_prefix}06`;
  DROP TABLE IF EXISTS `${temp_table_prefix}07`;
  DROP TABLE IF EXISTS `${temp_table_prefix}08`;
  DROP TABLE IF EXISTS `${temp_table_prefix}09`;

END;