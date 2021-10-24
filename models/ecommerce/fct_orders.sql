with orders as (
    select * from {{ source("ecommerce", "orders") }}
),
order_items as (
    select * from {{ source("ecommerce", "order_items" )}}
)

select * 

from order_items
left join orders using(order_id)