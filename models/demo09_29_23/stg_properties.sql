With source As (
    Select
        *,
        {{ standardize_address('Address') }}
    From {{ ref('property_information') }}
)

Select
    property_name,
    market,
    property_type,
    floor_area,
    address,
    standardized_address_final as standardized_address

From source
