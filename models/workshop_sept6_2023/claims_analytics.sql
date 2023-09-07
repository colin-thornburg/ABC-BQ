SELECT
    dim.patient_id,
    dim.patient_name,
    dim.date_of_birth,
    dim.provider_id,
    dim.provider_name,
    dim.specialty,
    fct.claim_id,
    fct.claim_date,
    fct.claim_amount,
    fct.total_claim_amount,
    fct.avg_claim_amount,
    fct.claim_count
FROM 
    {{ ref('abc_company','dim_patient_provider') }} dim
JOIN 
    {{ ref('abc_company','fct_claims') }} fct 
ON 
    dim.patient_id = fct.patient_id AND
    dim.provider_id = fct.provider_id