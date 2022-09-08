with Intermediate as (

    Select * from {{ ref('Int_Finance') }}
)

Select * from Intermediate
union all
Select 'Accounting Mart' as column_a