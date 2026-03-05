with source as (
    select * from {{ source('olist', 'order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id::int      as order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::timestamp as shipping_limit_at,
        price::float            as price,
        freight_value::float    as freight_value
    from source
)

select * from renamed
