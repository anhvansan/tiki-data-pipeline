{{
    config(
        materialized='table'
    )
}}

with product_daily as (
    select *
    from {{ ref('mart_product_daily') }}
)

select
    snapshot_date,
    primary_category_name,

    -- Product metrics
    count(distinct product_id) as product_count,

    -- Price metrics
    round(avg(price), 2) as avg_price,
    round(avg(original_price), 2) as avg_original_price,
    round(avg(discount_pct), 2) as avg_discount_pct,

    -- Promotion metrics
    countif(is_promotion) as promoted_product_count,
    round(
        safe_divide(
            countif(is_promotion),
            count(distinct product_id)
        ) * 100,
        2
    ) as promotion_rate,

    -- Product performance
    round(avg(rating_average), 2) as avg_rating,
    sum(review_count) as total_review_count,
    sum(quantity_sold) as total_quantity_sold,

    -- Availability
    countif(availability = 1) as available_product_count,
    round(
        safe_divide(
            countif(availability = 1),
            count(distinct product_id)
        ) * 100,
        2
    ) as availability_rate,

    -- Price movement
    round(avg(price_change_pct), 2) as avg_price_change_pct

from product_daily
where primary_category_name is not null
group by
    snapshot_date,
    primary_category_name