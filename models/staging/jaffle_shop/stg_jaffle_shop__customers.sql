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

, rename_fields as (
    select
        stg_jaffle_shop__customers_id
        , id as customer_id
        , first_name as customer_first_name
        , last_name as customer_last_name
    from add_surrogate_key
)

select
    *
from rename_fields