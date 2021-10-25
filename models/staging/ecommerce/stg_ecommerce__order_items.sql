with 
order_items as (
    select * from {{ source('ecommerce', 'order_items') }}
),

valid_orders as (
    select
        user_id, 
        count(distinct order_id) as order_count, 
        sum(sale_price) as total_revenue,
        min(created_at) as first_ordered_timestamp,
        max(created_at) as last_ordered_timestamp,
        count(distinct format_timestamp('%y-%m', created_at)) as active_month_count,
        sum(sale_price) / nullif(count(distinct order_id), 0) as average_order_value
    from order_items
    where status not in ('Cancelled', 'Returned')
    group by user_id
),

cancelled_orders as (
    select 
        user_id, 
        count(distinct order_id) as cancelled_order_count, 
        sum(sale_price) as cancelled_revenue,
    from order_items
    where status = 'Cancelled'
    group by user_id 
),
returned_orders as (
    select 
        user_id, 
        count(distinct order_id) as returned_order_count, 
        sum(sale_price) as returned_revenue,
    from order_items
    where status = 'Returned'
    group by user_id 
)

select
    *
from valid_orders 
left join cancelled_orders using (user_id)
left join returned_orders using(user_id)
