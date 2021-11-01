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
    where status not in ('Cancelled', 'Returned')
),

most_purchased_category as (
    select
        customer_id,
        product_category as most_purchased_category_name,
        sum(product_price) as total_most_purchased_category_revenue,
        count(distinct order_id) as most_purchased_category_order_count
        
    from orders
    group by 1, 2 
    qualify row_number() over(partition by customer_id order by sum(product_price) desc) = 1
),

final as (
    select 
        customer_id,
        sum(product_cost) as total_cogs,
        sum(product_price) - sum(product_cost) as total_margin,
        count(distinct product_id) as unique_items_purchased_count,
        sum(product_price) / nullif(count(distinct product_id), 0) as total_revenue_per_item

    from orders
    group by 1
)

select * from final
left join most_purchased_category using (customer_id)