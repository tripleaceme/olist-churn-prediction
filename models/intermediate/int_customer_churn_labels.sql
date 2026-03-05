-- Churn definition for marketplace data:
-- A customer is "churned" if they made only 1 order (never returned).
-- A customer is "retained" if they placed 2+ orders.
-- We only label customers whose last order is >= 90 days before dataset end
-- to ensure sufficient observation window.

with customer_orders as (
    select * from {{ ref('int_customer_order_summary') }}
),

dataset_boundary as (
    select max(last_order_at) as max_order_date
    from customer_orders
),

labeled as (
    select
        co.customer_unique_id,
        co.total_orders,
        co.last_order_at,
        db.max_order_date,
        datediff('day', co.last_order_at, db.max_order_date) as days_since_last_order,
        case
            when datediff('day', co.last_order_at, db.max_order_date) >= 90 then true
            else false
        end as has_sufficient_window,
        case
            when co.total_orders = 1 then 1  -- single purchase = churned
            else 0                            -- repeat buyer = retained
        end as churned
    from customer_orders co
    cross join dataset_boundary db
)

select *
from labeled
where has_sufficient_window = true
