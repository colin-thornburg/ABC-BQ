
{{ config(
    materialized='incremental',
    unique_key='comp_id'
) }}

-- Intermediate model to join tenants, properties, and leasing data incrementally
WITH tenant_property AS (
  SELECT
    t.*,
    p.property_name,
    p.property_type,
    p.floor_area
  FROM {{ ref('stg_tenants') }} t
  LEFT JOIN {{ ref('stg_properties') }} p
  ON t.standardized_address = p.standardized_address
),

leasing AS (
  SELECT
    comp_id,
    lease_price,
   TO_TIMESTAMP(lease_date) AS lease_date,
    standardized_address
  FROM {{ ref('stg_leasing') }}
)

SELECT
  tp.*,
  l.comp_id,
  l.lease_price,
  l.lease_date,
  CASE
    WHEN l.standardized_address IS NULL THEN 'No Match'
    ELSE 'Match'
  END AS lease_match_status
FROM tenant_property tp
LEFT JOIN leasing l
ON tp.standardized_address = l.standardized_address

{% if is_incremental() %}
-- apply filter to only include new or updated records in the incremental run
WHERE l.comp_id >= (select max(comp_id) from {{ this }})
{% endif %}

