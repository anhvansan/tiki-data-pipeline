with source as (
    select * from {{ source('staging', 'products') }}
),

-- 1. Lọc batch mới nhất ngay từ nguồn thô để tối ưu RAM/Bytes Scanned
latest_batch as (
    select *
    from source
    where snapshot_date = (
        {% if var('snapshot_date', none) %}
            date('{{ var("snapshot_date") }}')
        {% else %}
            (select max(snapshot_date) from source)
        {% endif %}
    )
),

renamed as (
    select
        cast(id as int64)                      as product_id,
        sku                                     as product_sku,
        name                                    as product_name,
        url_key                                 as product_url_key,
        cast(availability as int64)             as availability,
        cast(seller_id as int64)                as seller_id,
        seller_name,
        brand_name,
        cast(price as float64)                  as price,
        cast(original_price as float64)         as original_price,
        cast(discount as float64)               as discount_amount,
        cast(discount_rate as float64)          as discount_rate,
        cast(rating_average as float64)         as rating_average,
        cast(review_count as int64)             as review_count,
        cast(quantity_sold as int64)            as quantity_sold,
        primary_category_name,
        primary_category_path,
        cast(snapshot_date as date)             as snapshot_date
    from latest_batch
),

-- 2. Deduplicate bảo vệ Primary Key cho snapshot
deduped as (
    select * from renamed
    qualify row_number() over (partition by product_id order by price desc) = 1
)

select * from deduped