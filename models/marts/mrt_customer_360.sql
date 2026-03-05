{{
    config(
        materialized='table',
        tags=['marts', 'ml']
    )
}}

with order_summary as (
    select * from {{ ref('int_customer_order_summary') }}
),

rfm as (
    select * from {{ ref('int_customer_rfm') }}
),

behavioral as (
    select * from {{ ref('int_customer_behavioral_features') }}
),

labels as (
    select * from {{ ref('int_customer_churn_labels') }}
)

select
    os.customer_unique_id,
    os.state,
    os.total_orders,
    os.lifetime_revenue,
    os.avg_order_value,
    os.total_items_purchased,
    os.avg_delivery_days,
    os.avg_delivery_delta_days,
    os.avg_review_score,
    os.worst_review_score,
    os.avg_installments,
    os.late_delivery_count,
    datediff('day', os.first_order_at, os.last_order_at) as customer_tenure_days,

    -- RFM
    r.recency_days,
    r.frequency,
    r.monetary,
    r.recency_score,
    r.frequency_score,
    r.monetary_score,
    r.rfm_combined_score,

    -- Behavioral
    b.unique_categories_purchased,
    b.unique_products_purchased,
    b.unique_payment_methods,
    b.credit_card_usage_ratio,
    b.boleto_usage_ratio,

    -- Label
    l.days_since_last_order,
    l.churned

from order_summary os
inner join rfm r on os.customer_unique_id = r.customer_unique_id
inner join labels l on os.customer_unique_id = l.customer_unique_id
left join behavioral b on os.customer_unique_id = b.customer_unique_id
