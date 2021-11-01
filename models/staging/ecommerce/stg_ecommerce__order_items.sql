with order_items as (
    select * from {{ ref('order_items') }}
),

products as (
    select * from {{ ref('products') }}
),

orders as (
    select * from {{ ref('orders') }}
    left join order_items using(order_id)
    left join products using(product_id)
),


valid_orders as (
    select
        customer_id, 
        count(distinct order_id) as order_count, 
        sum(product_price) as total_revenue,
        min(created_at) as first_ordered_timestamp,
        max(created_at) as last_ordered_timestamp,
        count(distinct format_timestamp('%y-%m', created_at)) as active_month_count,
        sum(product_price) / nullif(count(distinct order_id), 0) as average_order_value
    from orders
    where status not in ('Cancelled', 'Returned')
    group by customer_id
),

cancelled_orders as (
    select 
        customer_id, 
        count(distinct order_id) as cancelled_order_count, 
        sum(product_price) as cancelled_revenue,
    from orders
    where status = 'Cancelled'
    group by customer_id 
),
returned_orders as (
    select 
        customer_id, 
        count(distinct order_id) as returned_order_count, 
        sum(product_price) as returned_revenue,
    from orders
    where status = 'Returned'
    group by customer_id 
)

select
    *
from valid_orders 
left join cancelled_orders using (customer_id)
left join returned_orders using(customer_id)
