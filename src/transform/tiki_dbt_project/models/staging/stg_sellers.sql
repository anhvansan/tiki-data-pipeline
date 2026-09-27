-- stg_sellers.sql
with source as (
    select * from {{ ref('stg_products_history') }}
),

-- Lấy tên mới nhất của mỗi seller trong toàn bộ lịch sử tích lũy
deduped as (
    select
        seller_id,
        seller_name
    from source
    where seller_id is not null
        and seller_name is not null
        and trim(seller_name) != ''
    qualify row_number() over (
        partition by seller_id 
        order by snapshot_date desc, seller_name
    ) = 1
)

select * from deduped