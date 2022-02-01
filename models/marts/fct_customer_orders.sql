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
        order_id, 
        max(payment_created_at) as payment_finalized_date, 
        sum(payment_amount) / 100.0 as total_amount_paid
    from import_payments
    group by 1
)

---Logical
, paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.customer_first_name,
        c.customer_last_name
    from import_orders as orders
    left join total_payments p
        on orders.order_id = p.order_id
    left join import_customers c 
        on orders.customer_id = c.customer_id 
)

, customer_orders as (
    select
        c.customer_id
        , min(orders.order_placed_at) as first_order_date
        , max(orders.order_placed_at) as most_recent_order_date
        , count(orders.order_id) as number_of_orders
    from import_customers c 
    left join import_orders as orders
        on orders.customer_id = c.customer_id 
    group by 1
)

, customer_lifetime_value as (
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 
        on p.customer_id = t2.customer_id 
        and p.order_id >= t2.order_id
    group by 1
)
---Final
, final as (
    select
        p.*,
        row_number() over (order by p.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
        case 
            when c.first_order_date = p.order_placed_at then 'new'
            else 'return' 
            end as nvsr,
        x.clv_bad as customer_lifetime_value,
        c.first_order_date as fdos
    from paid_orders p
    left join customer_orders as c 
        using (customer_id)
    left outer join customer_lifetime_value x 
        on x.order_id = p.order_id
)
---Simple Select
select
    *   
from final
