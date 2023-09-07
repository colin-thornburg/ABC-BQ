SELECT
    Claim_ID as claim_id,
    Patient_ID as patient_id,
    Provider_ID as provider_id,
    Claim_Date as claim_date,
    Claim_Amount as claim_amount,
    loaded_at as loaded_at_timestamp
FROM 
    {{ source('health_claims', 'claims') }}
