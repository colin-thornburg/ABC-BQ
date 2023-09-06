SELECT
    Patient_ID as patient_id,
    Patient_Name as patient_name,
    Date_of_Birth as date_of_birth,
    loaded_at as loaded_at_timestamp
FROM 
    {{ ref('patients') }}
