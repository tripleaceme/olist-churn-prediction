with customers as (
    select * from {{ ref('stg_olist__customers') }}
),

orders as (
    select * from {{ ref('int_order_enriched') }}
),

customer_orders as (
    select
        c.customer_unique_id,
        min(c.city)                                                    as city,
        min(c.state)                                                   as state,
        count(distinct o.order_id)                                     as total_orders,
        min(o.purchased_at)                                            as first_order_at,
        max(o.purchased_at)                                            as last_order_at,
        sum(o.total_payment)                                           as lifetime_revenue,
        avg(o.total_payment)                                           as avg_order_value,
        sum(o.item_count)                                              as total_items_purchased,
        avg(o.delivery_days)                                           as avg_delivery_days,
        avg(o.delivery_delta_days)                                     as avg_delivery_delta_days,
        avg(o.avg_review_score)                                        as avg_review_score,
        min(o.min_review_score)                                        as worst_review_score,
        avg(o.max_installments)                                        as avg_installments,
        count(case when o.delivery_delta_days < 0 then 1 end)         as late_delivery_count
    from customers c
    inner join orders o on c.customer_id = o.customer_id
    group by 1
)

select * from customer_orders
