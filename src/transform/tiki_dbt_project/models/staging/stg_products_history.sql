-- stg_products_history.sql

with source as (
    select * from {{ source('staging', 'products') }}
),

renamed as (
    select
        cast(id as int64)                      as product_id,
        cast(seller_id as int64)               as seller_id,
        seller_name,
        brand_name,
        primary_category_name,
        cast(price as float64)                 as price,
        cast(original_price as float64)        as original_price,
        cast(discount as float64)              as discount_amount,
        cast(discount_rate as float64)         as discount_rate,
        cast(availability as int64)            as availability,
        cast(rating_average as float64)        as rating_average,
        cast(review_count as int64)             as review_count,
        cast(quantity_sold as int64)            as quantity_sold,
        cast(snapshot_date as date)             as snapshot_date
    from source
),

-- Đảm bảo đúng Grain (product_id, snapshot_date)
deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by product_id, snapshot_date
        order by price desc
    ) = 1
)

select * from deduped
