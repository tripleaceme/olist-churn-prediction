with source as (
    select * from {{ source('olist', 'products') }}
),

translated as (
    select * from {{ ref('product_category_name_translation') }}
),

renamed as (
    select
        p.product_id,
        coalesce(t.product_category_name_english, p.product_category_name) as product_category,
        p.product_name_lenght::int           as product_name_length,
        p.product_description_lenght::int    as product_description_length,
        p.product_photos_qty::int            as product_photos_count,
        p.product_weight_g::int              as product_weight_grams,
        p.product_length_cm::int             as product_length_cm,
        p.product_height_cm::int             as product_height_cm,
        p.product_width_cm::int              as product_width_cm
    from source p
    left join translated t
        on p.product_category_name = t.product_category_name
)

select * from renamed
