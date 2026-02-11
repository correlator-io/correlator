-- Customers mart model
-- Aggregates customer data with order statistics
-- DEPENDS ON marts.orders (cascading dependency pattern)

with customers as (
    select * from {{ ref('stg_customers') }}
),

-- Reference the MART orders, not staging orders
-- This creates a realistic cascading dependency:
-- stg_orders -> marts.orders -> marts.customers
orders as (
    select * from {{ ref('orders') }}
),

customer_orders as (
    select
        customer_id,
        count(*) as order_count,
        sum(order_total) as total_amount,
        min(ordered_at) as first_order_at,
        max(ordered_at) as last_order_at
    from orders
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.customer_name,
        coalesce(co.order_count, 0) as order_count,
        coalesce(co.total_amount, 0) as total_amount,
        co.first_order_at,
        co.last_order_at
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from final
