with orders as (
    select 
        *,
        date_diff(order_delivered_carrier_date, order_estimated_delivery_date, day) as delivery_vs_expectation_days,
        date_diff(order_delivered_carrier_date, order_purchase_timestamp, day) as ordered_to_delivered_days
    
    from {{ source("ecommerce", "orders") }}
),
final as (
    select 
        customer_id,
        count(distinct order_id) as order_count,
        count(distinct case when order_status = 'delivered' then order_id end) as order_delivered_count,
        min(order_purchase_timestamp) as first_order_date,
        max(order_purchase_timestamp) as last_order_date,
        count(distinct format_date("%Y-%m", order_purchase_timestamp)) as active_month_count

    from orders
    group by customer_id
)

select * from final