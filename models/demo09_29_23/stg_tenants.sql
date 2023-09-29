With source As (
    Select *,
        {{ standardize_address('Address') }}
    From {{ ref('tenant_information') }}
)

Select
    tenant_id,
    tenant_name,
    industry,
    lease_start_date,
    lease_end_date,
    address,
    standardized_address_final as standardized_address
From source
