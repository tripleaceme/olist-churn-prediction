with order_items as (
    select
        c.customer_unique_id,
        oi.product_id,
        p.product_category
    from {{ ref('stg_olist__order_items') }} oi
    inner join {{ ref('stg_olist__orders') }} o on oi.order_id = o.order_id
    inner join {{ ref('stg_olist__customers') }} c on o.customer_id = c.customer_id
    inner join {{ ref('stg_olist__products') }} p on oi.product_id = p.product_id
    where o.order_status = 'delivered'
),

payments as (
    select
        c.customer_unique_id,
        op.payment_type
    from {{ ref('stg_olist__order_payments') }} op
    inner join {{ ref('stg_olist__orders') }} o on op.order_id = o.order_id
    inner join {{ ref('stg_olist__customers') }} c on o.customer_id = c.customer_id
    where o.order_status = 'delivered'
),

product_features as (
    select
        customer_unique_id,
        count(distinct product_category) as unique_categories_purchased,
        count(distinct product_id)       as unique_products_purchased
    from order_items
    group by 1
),

payment_features as (
    select
        customer_unique_id,
        count(distinct payment_type)                                        as unique_payment_methods,
        sum(case when payment_type = 'credit_card' then 1 else 0 end)::float
            / nullif(count(*), 0)                                           as credit_card_usage_ratio,
        sum(case when payment_type = 'boleto' then 1 else 0 end)::float
            / nullif(count(*), 0)                                           as boleto_usage_ratio
    from payments
    group by 1
)

select
    pf.customer_unique_id,
    pf.unique_categories_purchased,
    pf.unique_products_purchased,
    pay.unique_payment_methods,
    pay.credit_card_usage_ratio,
    pay.boleto_usage_ratio
from product_features pf
left join payment_features pay on pf.customer_unique_id = pay.customer_unique_id
