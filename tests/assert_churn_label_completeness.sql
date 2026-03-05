-- Ensure all customers with sufficient observation window have a churn label
with customer_orders as (
    select * from {{ ref('int_customer_order_summary') }}
),

labels as (
    select * from {{ ref('int_customer_churn_labels') }}
),

dataset_boundary as (
    select max(last_order_at) as max_order_date
    from customer_orders
),

missing_labels as (
    select co.customer_unique_id
    from customer_orders co
    cross join dataset_boundary db
    left join labels l on co.customer_unique_id = l.customer_unique_id
    where datediff('day', co.last_order_at, db.max_order_date) >= 90
      and l.customer_unique_id is null
)

select * from missing_labels
