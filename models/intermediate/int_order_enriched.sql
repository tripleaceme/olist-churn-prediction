with orders as (
    select * from {{ ref('stg_olist__orders') }}
    where order_status = 'delivered'
),

items as (
    select
        order_id,
        count(*)                          as item_count,
        count(distinct product_id)        as unique_products,
        count(distinct seller_id)         as unique_sellers,
        sum(price)                        as total_price,
        sum(freight_value)                as total_freight
    from {{ ref('stg_olist__order_items') }}
    group by 1
),

payments as (
    select
        order_id,
        count(distinct payment_type)      as payment_type_count,
        max(payment_installments)         as max_installments,
        sum(payment_value)                as total_payment
    from {{ ref('stg_olist__order_payments') }}
    group by 1
),

reviews as (
    select
        order_id,
        avg(review_score)                 as avg_review_score,
        min(review_score)                 as min_review_score
    from {{ ref('stg_olist__order_reviews') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    o.purchased_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_at,
    datediff('day', o.purchased_at, o.delivered_at)          as delivery_days,
    datediff('day', o.purchased_at, o.estimated_delivery_at) as estimated_delivery_days,
    datediff('day', o.delivered_at, o.estimated_delivery_at) as delivery_delta_days,
    i.item_count,
    i.unique_products,
    i.unique_sellers,
    i.total_price,
    i.total_freight,
    p.payment_type_count,
    p.max_installments,
    p.total_payment,
    r.avg_review_score,
    r.min_review_score
from orders o
left join items i on o.order_id = i.order_id
left join payments p on o.order_id = p.order_id
left join reviews r on o.order_id = r.order_id
