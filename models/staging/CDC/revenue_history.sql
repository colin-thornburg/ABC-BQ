--id config not present --> result will append only behavior      

{{
    config(
      materialized='incremental',
      full_refresh=false,
      schema='history',
      on_schema_change='sync_all_columns'
    )
}}

Select * from {{ ref('revenue') }}
{% if is_incremental() %}
    where run_timestamp >= (select max(run_timestamp) from {{ this }})
{% endif %}