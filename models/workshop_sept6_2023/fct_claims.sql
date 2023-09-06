SELECT
    c.claim_id,
    p.patient_id,
    p.patient_name,
    pr.provider_id,
    pr.provider_name,
    pr.specialty,
    c.claim_date,
    c.claim_amount,
    SUM(c.claim_amount) OVER(PARTITION BY pr.provider_id) as total_claim_amount,
    AVG(c.claim_amount) OVER(PARTITION BY pr.provider_id) as avg_claim_amount,
    COUNT(c.claim_id) OVER(PARTITION BY pr.provider_id) as claim_count
FROM 
    {{ ref('stg_patients') }} p
JOIN 
    {{ ref('stg_claims') }} c ON p.patient_id = c.patient_id
JOIN 
    {{ ref('stg_providers') }} pr ON pr.provider_id = c.provider_id
