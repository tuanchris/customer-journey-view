with events as (
    select * from {{ source('ecommerce', 'events') }}
    where user_id is not null
),

sessions as (
    select 
        user_id, 
        session_id,
        timestamp_diff(max(created_at), min(created_at), minute) as session_length
    from events
    where user_id is not null
    group by user_id, session_id
),

user_sessions as (
    select
        user_id,
        avg(session_length) as average_session_length_minutes
    from sessions
    group by 1
),

final as (
    select
        user_id,
        count(distinct session_id) as session_count,
        count(distinct case when event_type = "Cart" then session_id end) as session_with_cart_count,
        count(distinct case when event_type = "Purchase" then session_id end) as session_with_purchase_count,
        min(created_at) as first_session_timestamp,
        max(created_at) as last_session_timestamp,
        count(distinct os) as unique_os_count,
        count(distinct ip_address) as unique_ip_count,
        
    from events
    group by user_id
)
select * from final
left join user_sessions using(user_id)