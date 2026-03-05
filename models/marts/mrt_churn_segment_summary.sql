{{
    config(
        materialized='table',
        tags=['marts', 'dashboard']
    )
}}

with predictions as (
    select * from {{ ref('mrt_churn_prediction') }}
)

select
    churn_risk_tier,
    state,
    count(*)                                      as customer_count,
    sum(case when churned = 1 then 1 else 0 end)  as actual_churned_count,
    avg(churn_probability)                         as avg_churn_probability,
    avg(lifetime_revenue)                          as avg_lifetime_revenue,
    avg(total_orders)                              as avg_total_orders,
    avg(avg_review_score)                          as avg_review_score,
    avg(recency_days)                              as avg_recency_days,
    sum(lifetime_revenue)                          as total_revenue_at_risk
from predictions
group by 1, 2
