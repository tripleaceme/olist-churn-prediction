{{
    config(
        materialized='table',
        tags=['marts', 'analytics']
    )
}}

with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
    where order_status = 'delivered'
),

reviews as (
    select * from {{ ref('stg_olist__order_reviews') }}
),

sellers as (
    select * from {{ ref('stg_olist__sellers') }}
),

seller_metrics as (
    select
        s.seller_id,
        s.city                                          as seller_city,
        s.state                                         as seller_state,
        count(distinct oi.order_id)                     as total_orders_fulfilled,
        count(distinct oi.product_id)                   as unique_products_sold,
        sum(oi.price)                                   as total_revenue,
        avg(oi.price)                                   as avg_item_price,
        avg(oi.freight_value)                            as avg_freight_value,
        avg(r.review_score)                              as avg_review_score,
        avg(datediff('day', o.purchased_at, o.delivered_at)) as avg_delivery_days,
        sum(case when o.delivered_at > o.estimated_delivery_at then 1 else 0 end)::float
            / nullif(count(distinct oi.order_id), 0)    as late_delivery_rate
    from sellers s
    inner join order_items oi on s.seller_id = oi.seller_id
    inner join orders o on oi.order_id = o.order_id
    left join reviews r on o.order_id = r.order_id
    group by 1, 2, 3
)

select * from seller_metrics
