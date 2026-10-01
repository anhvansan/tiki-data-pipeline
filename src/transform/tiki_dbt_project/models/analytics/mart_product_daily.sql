{{
    config(
        materialized='table'
    )
}}

with fact as (
    select
        product_id,
        snapshot_date,
        seller_id,
        primary_category_name,
        price,
        original_price,
        discount_amount,
        discount_rate,
        discount_pct,
        is_promotion,
        availability,
        rating_average,
        review_count,
        quantity_sold,
        price_change_pct,
        days_since_last_price_change
    from {{ ref('fact_product_snapshot') }}
),

product as (
    select
        product_id,
        product_name,
        product_sku,
        brand_name
    from {{ ref('dim_product') }}
    qualify row_number() over (
        partition by product_id
        order by valid_from desc
    ) = 1
)

select
    f.snapshot_date,
    f.product_id,
    p.product_sku,
    p.product_name,
    p.brand_name,
    f.seller_id,
    f.primary_category_name,

    -- Price
    f.price,
    f.original_price,
    f.discount_amount,
    f.discount_rate,
    f.discount_pct,

    -- Promotion
    f.is_promotion,

    -- Product performance
    f.availability,
    f.rating_average,
    f.review_count,
    f.quantity_sold,

    -- Price trend
    f.price_change_pct,
    f.days_since_last_price_change

from fact f
left join product p
    on f.product_id = p.product_id