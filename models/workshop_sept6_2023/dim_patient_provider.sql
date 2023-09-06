{{
    config(
        materialized='table'
    )
}}

SELECT
    p.patient_id,
    p.patient_name,
    p.date_of_birth,
    pr.provider_id,
    pr.provider_name,
    pr.specialty
FROM 
    {{ ref('stg_patients') }} p
JOIN 
    {{ ref('stg_claims') }} c ON p.patient_id = c.patient_id
JOIN 
    {{ ref('stg_providers') }} pr ON pr.provider_id = c.provider_id
