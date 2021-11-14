with events as (
    select * from {{ ref('src_google_analytics__events') }}
    where customer_id is not null
),

final as (
    select
        customer_id,
        count(distinct session_id) as session_count,
        count(distinct case when event_type = "Add to Cart" then session_id end) as session_with_cart_count,
        count(distinct case when event_type = "Checkout" then session_id end) as session_with_checkout_count,
        min(created_at) as first_session_timestamp,
        max(created_at) as last_session_timestamp,
        count(distinct ip_address) as unique_ip_count,
        
    from events
    group by customer_id
)
select * from final
