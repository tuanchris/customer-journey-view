with users_info as (
    select 
        id as user_id,
        created_at as customer_signedup_date,
        * except(id, created_at) 
    from {{ source('ecommerce', 'users') }}
),

event_metrics as (
    select * from {{ ref('stg_ecommerce__events') }}
),

product_metrics as (
    select * from {{ ref('stg_ecommerce__inventory_items') }}
),

order_metrics as (
    select * from {{ ref('stg_ecommerce__order_items') }}
),

final as (
    select 
        users_info.*,
        order_metrics.* except(user_id),
        product_metrics.* except(user_id),
        event_metrics.* except(user_id),
        safe_divide(order_count, active_month_count) as order_per_active_month,
        safe_divide(total_margin, total_revenue) as customer_margin_rate,
        safe_divide(unique_items_purchased_count, order_count) as average_items_per_order,
        safe_divide(session_with_cart_count, session_count) as session_with_cart_rate,
        safe_divide(session_with_purchase_count, session_count) as session_with_purchase_rate,
        timestamp_diff(first_ordered_timestamp, customer_signedup_date, day) as days_to_first_purchase,
        timestamp_diff(current_timestamp(), last_ordered_timestamp, day) as days_since_last_purchase

    from users_info
    left join order_metrics using(user_id)
    left join product_metrics using(user_id)
    left join event_metrics using(user_id)

)

select * from final