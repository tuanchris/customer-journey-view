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

events as (
    select * from {{ ref('events') }}
),

unioned as (
    select
        {{ dbt_utils.surrogate_key(["customer_id", "session_id"]) }} as id,
        customer_id,
        session_id as event_id,
        min(created_at) created_at,
        "completed" as event_status,
        "web_event" as event_type,
        array_agg(event_type order by created_at asc) as event_value
    from events
    where customer_id is not null
    group by 1, 2, 3

    union all

    select
        {{ dbt_utils.surrogate_key(["customer_id", "order_id"]) }} as id,
        customer_id, 
        order_id as event_id,
        min(created_at) as created_at,
        max(status) as event_status, 
        "order" as event_type,
        array_agg(ifnull(product_name, "")) event_value
    from orders
    group by 1, 2, 3
)

select * from unioned
order by customer_id, created_at desc
