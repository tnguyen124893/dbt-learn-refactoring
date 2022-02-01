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

, rename_fields as (
    select
        stg_jaffle_shop__orders_id
        , id as order_id
        , user_id as customer_id
        , order_date as order_placed_at
        , status as order_status
    from add_surrogate_key
)

select
    *
from rename_fields