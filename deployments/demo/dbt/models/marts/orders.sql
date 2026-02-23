-- Orders mart model
-- Enriches orders with customer information
-- This is the FIRST mart to be built (no mart dependencies)

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.customer_name,
        o.ordered_at,
        o.store_id,
        o.subtotal,
        o.tax_paid,
        o.order_total
    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from final
