with import_orders as (
    select
        *
    from {{source('dbt_tung','orders')}}
)
, add_surrogate_key as (
    select
        {{dbt_utils.surrogate_key([
            'id'
            , 'user_id'
            , 'order_date'
            , 'status'
        ])}} as stg_jaffle_shop__orders_id
        , *
    from import_orders
)

select
    *
from add_surrogate_key