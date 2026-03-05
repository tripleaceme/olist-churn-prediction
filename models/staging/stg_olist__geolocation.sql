with source as (
    select * from {{ source('olist', 'geolocation') }}
),

deduplicated as (
    select
        geolocation_zip_code_prefix::varchar as zip_code,
        avg(geolocation_lat::float)          as latitude,
        avg(geolocation_lng::float)          as longitude,
        max(geolocation_city)                as city,
        max(geolocation_state)               as state
    from source
    group by 1
)

select * from deduplicated
