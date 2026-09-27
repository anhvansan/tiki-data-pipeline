with source as (
    select * from {{ ref('stg_products_history') }}
    where brand_id is not null
),

-- Đảm bảo mỗi brand_id chỉ ra 1 dòng duy nhất và không bị mất brand cũ
deduped as (
    select
        brand_id,
        brand_name
    from source
    qualify row_number() over (partition by brand_id order by snapshot_date desc, brand_name) = 1
)

select * from deduped