with source as (

    Select * from {{ ref('Source_Finance') }}
)

Select * from source
union all
Select 'Intermediate Finance' as column_a