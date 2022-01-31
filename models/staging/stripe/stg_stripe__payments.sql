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
        ])}} as stg_strip__payments_id
        , *
    from import_payments
)

select
    *
from add_surrogate_key