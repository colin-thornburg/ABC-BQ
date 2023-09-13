{{
    config(
        materialized='view'
    )
}}

Select * from {{ ref('claims_analytics') }}