with events as (
    select * from {{ source('ecommerce', 'events') }}
),

order_items as (
    select * from {{ source('ecommerce', 'order_items') }}
),

inventory_items as (
    select * from {{ source('ecommerce', 'inventory_items') }}
),

unioned as (
    select
        {{ dbt_utils.surrogate_key(["user_id", "session_id"]) }} as id,
        user_id,
        session_id as event_id,
        min(created_at) created_at,
        "completed" as event_status,
        "web_event" as event_type,
        array_agg(event_type order by created_at asc) as event_value
    from events
    where user_id is not null
    group by 1, 2, 3

    union all

    select
        {{ dbt_utils.surrogate_key(["user_id", "order_id"]) }} as id,
        user_id, 
        cast(order_id as string) as event_id,
        min(order_items.created_at) as created_at,
        max(status) as event_status, 
        "order" as event_type,
        array_agg(ifnull(product_name, "")) event_value
    from order_items
    left join inventory_items on inventory_items.id = order_items.id
    group by 1, 2, 3
)

select * from unioned
order by user_id, created_at desc
