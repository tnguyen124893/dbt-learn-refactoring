with 
---Import
import_orders as (
    select
        *
    from {{ref('stg_jaffle_shop__orders')}}
)
, import_customers as (
    select
        *
    from {{ref('stg_jaffle_shop__customers')}}
)

, import_payments as (
    select
        *
    from {{ref('stg_stripe__payments')}}
    where 1=1
    and payment_status <> 'fail'
)

, total_payments as (
    select 
        order_id
        , max(payment_created_at) as payment_finalized_date
        , sum(payment_amount) / 100.0 as total_amount_paid
    from import_payments
    group by 1
)

---Logical
, paid_orders as (
    select
        import_orders.order_id
        , import_orders.customer_id
        , import_orders.order_placed_at
        , import_orders.order_status
        , total_payments.total_amount_paid
        , total_payments.payment_finalized_date
    from import_orders
    left join total_payments
        on import_orders.order_id = total_payments.order_id
)

---Final
, final as (
    select
    ---orders
        paid_orders.order_id
        , paid_orders.customer_id
        , paid_orders.order_placed_at
        , paid_orders.order_status
        , row_number() over (order by paid_orders.order_id) as transaction_seq
        , row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq
        , case 
            when row_number() over (partition by paid_orders.customer_id order by paid_orders.order_placed_at asc) = 1 then 'new'
            else 'return'
            end as nvsr
    ---payment
        , paid_orders.payment_finalized_date
        , paid_orders.total_amount_paid
    ---customer
        , import_customers.customer_first_name
        , import_customers.customer_last_name
        , min(paid_orders.order_placed_at) over (partition by paid_orders.customer_id) as fdos
        , sum(paid_orders.total_amount_paid) over (partition by paid_orders.customer_id order by paid_orders.order_placed_at asc) as customer_lifetime_value
    from paid_orders
    left join import_customers
        using (customer_id)
)
---Simple Select
select
    *   
from final
