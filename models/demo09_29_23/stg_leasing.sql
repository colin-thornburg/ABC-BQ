With source As (
    Select
        *,
        {{ standardize_address('Address') }}
    From {{ ref('lease_comps_information') }}
)

Select
    comp_id,
    lease_price,
    lease_date,
    address,
    standardized_address_final As standardized_address

From source
