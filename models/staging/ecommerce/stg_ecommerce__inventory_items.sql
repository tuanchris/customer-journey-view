with order_items as (
    select * from {{ source('ecommerce', 'order_items') }}
    where status not in ('Cancelled', 'Returned')
),

inventory_items as (
    select * from {{ source('ecommerce', 'inventory_items') }}
),

most_purchased_category as (
    select
        user_id,
        product_category as most_purchased_category_name,
        sum(sale_price) as total_most_purchased_category_revenue,
        count(distinct order_id) as most_purchased_category_order_count
        
    from order_items
    left join inventory_items on order_items.inventory_item_id = inventory_items.id
    group by 1, 2 
    qualify row_number() over(partition by user_id order by sum(sale_price) desc) = 1
),

final as (
    select 
        user_id,
        sum(cost) as total_cogs,
        sum(sale_price) - sum(cost) as total_margin,
        count(distinct product_id) as unique_items_purchased_count,
        sum(sale_price) / nullif(count(distinct product_id), 0) as total_revenue_per_item


    from order_items
    left join inventory_items on order_items.inventory_item_id = inventory_items.id
    group by 1
)

select * from final
left join most_purchased_category using (user_id)