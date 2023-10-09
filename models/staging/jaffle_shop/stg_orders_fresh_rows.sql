With source as (
Select * from {{ source('jaffle_shop', 'orders') }}
),

append_rows as (Select
1001 as ID,
1001 as USER_ID,
'2023-10-09' as order_date,
'completed' as State,
current_timestamp as _ETL_LOADED_AT) --- This is the trigger!!!---
Select * from source
union all
Select * from append_rows