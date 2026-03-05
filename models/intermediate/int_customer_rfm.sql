with customer_orders as (
    select * from {{ ref('int_customer_order_summary') }}
),

dataset_boundary as (
    select max(last_order_at) as max_order_date
    from customer_orders
),

rfm as (
    select
        co.customer_unique_id,
        datediff('day', co.last_order_at, db.max_order_date) as recency_days,
        co.total_orders                                       as frequency,
        co.lifetime_revenue                                   as monetary,
        ntile(5) over (order by datediff('day', co.last_order_at, db.max_order_date) desc) as recency_score,
        ntile(5) over (order by co.total_orders)              as frequency_score,
        ntile(5) over (order by co.lifetime_revenue)          as monetary_score
    from customer_orders co
    cross join dataset_boundary db
)

select
    *,
    recency_score + frequency_score + monetary_score as rfm_combined_score
from rfm
