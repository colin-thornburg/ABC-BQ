-- models/staging/stg_hl7_cmn_code.sql

WITH source AS (
    SELECT * FROM {{ source('edwhl7_staging_views', 'HL7_CMN_CODE') }}
),

renamed AS (
    SELECT
        CD_SSUKT,
        CD_SK,
        CD,
        CD_DESC,
        CD_ALT_DESC,
        SRS_SYS_REF_CD,
        EFCTV_FR_DT,
        EFCTV_TO_DT,
        {{ dbt_utils.generate_surrogate_key([
            'CD_SSUKT', 
            'SRS_SYS_REF_CD'
        ]) }} AS CMN_CODE_HASH_KEY,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_HL7_CMN_CODE_HASHID
    FROM source
)

SELECT * FROM renamed