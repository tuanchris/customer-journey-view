with order_items as (
    select * from {{ ref('src_shopify__order_items') }}
),

products as (
    select * from {{ ref('src_shopify__products') }}
),

orders as (
    select * from {{ ref('src_shopify__orders') }}
    left join order_items using(order_id)
    left join products using(product_id)
)

select * from orders