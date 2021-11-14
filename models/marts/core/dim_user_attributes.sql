with customers_info as (
    select * from {{ ref('src_hubspot__customers') }}
),

event_metrics as (
    select * from {{ ref('stg_ecommerce__events') }}
),

product_metrics as (
    select * from {{ ref('stg_ecommerce__products') }}
),

order_metrics as (
    select * from {{ ref('stg_ecommerce__order_items') }}
),

final as (
    select 
        customers_info.*,
        order_metrics.* except(customer_id),
        product_metrics.* except(customer_id),
        event_metrics.* except(customer_id),
        safe_divide(order_count, active_month_count) as order_per_active_month,
        safe_divide(total_margin, total_revenue) as customer_margin_rate,
        safe_divide(unique_items_purchased_count, order_count) as average_items_per_order,
        safe_divide(session_with_cart_count, session_count) as session_with_cart_rate,
        safe_divide(session_with_checkout_count, session_count) as session_with_checkout_rate,
        timestamp_diff(first_ordered_timestamp, customers_info.created_at, day) as days_to_first_purchase,
        timestamp_diff(datetime(current_timestamp()), last_ordered_timestamp, day) as days_since_last_purchase

    from customers_info
    left join order_metrics using(customer_id)
    left join product_metrics using(customer_id)
    left join event_metrics using(customer_id)

)

select * from final