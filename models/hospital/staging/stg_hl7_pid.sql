-- models/staging/stg_hl7_pid.sql

WITH source AS (
    SELECT * FROM {{ source('edwhl7_staging_views', 'HL7_PID') }}
),

renamed AS (
    /*
    This CTE renames and selects the columns we need from the source table.
    We're keeping all relevant fields that might be used in downstream models.
    */
    SELECT
        MESSAGE_CONTROL_ID,
        PATIENT_ID_EXTERNAL_ID,
        PATIENT_ID_INTERNAL_ID,
        PATIENT_ID_ALTERNATE_ID,
        PATIENT_NAME,
        MOTHER_MAIDEN_NAME,
        DATE_TIME_OF_BIRTH,
        SEX,
        PATIENT_ALIAS,
        RACE,
        PATIENT_ADDRESS,
        COUNTRY_CODE,
        PHONE_NUMBER_HOME,
        PHONE_NUMBER_BUSINESS,
        PRIMARY_LANGUAGE,
        MARITAL_STATUS,
        RELIGION,
        PATIENT_ACCOUNT_NUMBER,
        SSN_NUMBER_PATIENT,
        DRIVERS_LICENSE_NUMBER_PATIENT,
        MOTHERS_IDENTIFIER,
        ETHNIC_GROUP,
        BIRTH_PLACE,
        MULTIPLE_BIRTH_INDICATOR,
        BIRTH_ORDER,
        CITIZENSHIP,
        VETERANS_MILITARY_STATUS,
        NATIONALITY,
        PATIENT_DEATH_DATE_AND_TIME,
        PATIENT_DEATH_INDICATOR,
        MESSAGE_TYPE,
        ALTERNATE_PATIENT_ID_ID_NUM,
        PATIENT_ID_LIST_ID_TYPE_CODE,
        PATIENT_ID_LIST_ID_NUM,
        SOURCE_INTERFACE,
        ALTERNATE_PATIENT_ID_ID_TYPE_CODE,
        {{ dbt_utils.generate_surrogate_key([
            'MESSAGE_CONTROL_ID', 
            'PATIENT_ID_INTERNAL_ID'
        ]) }} AS PID_HASH_KEY,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_HL7_PID_HASHID
    FROM source
    WHERE MESSAGE_TYPE = 'ADT'  -- We're only interested in ADT messages
)

SELECT * FROM renamed