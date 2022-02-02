with import_payments as (
    select
        *
    from {{source('dbt_tung','payments')}}
)

, add_surrogate_key as (
    select
        {{dbt_utils.surrogate_key([
            'id'
            , 'orderid'
            , 'paymentmethod'
            , 'status'
            , 'amount'
            , 'created'
        ])}} as stg_stripe__payments_id
        , *
    from import_payments
)

, rename_fields as (
    select
        stg_stripe__payments_id
        , id as payment_id
        , orderid as order_id
        , paymentmethod as payment_method
        , status as payment_status
        , amount as payment_amount
        , created as payment_created_at
    from add_surrogate_key
)

select
    *
from rename_fields