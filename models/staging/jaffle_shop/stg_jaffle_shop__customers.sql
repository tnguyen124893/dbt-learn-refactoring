with import_customers as (
    select
        *
    from {{source('dbt_tung','customers')}}
)
, add_surrogate_key as (
    select
        {{dbt_utils.surrogate_key([
            'id'
            , 'first_name'
            , 'last_name'
        ])}} as stg_jaffle_shop__customers_id
        , *
    from import_customers
)

select
    *
from add_surrogate_key