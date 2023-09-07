SELECT
    Provider_ID as provider_id,
    Provider_Name as provider_name,
    Specialty as specialty,
    loaded_at as loaded_at_timestamp
FROM 
    {{ source('health_claims','providers') }}
