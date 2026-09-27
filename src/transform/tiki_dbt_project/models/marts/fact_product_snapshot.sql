{{
    config(
        materialized='incremental',
        unique_key='fact_key',
        on_schema_change='sync_all_columns'
    )
}}

-- fact_product_snapshot.sql
-- Grain: 1 dòng = 1 product_id tại 1 snapshot_date.

with history as (
    select * from {{ ref('stg_products_history') }}

    {% if is_incremental() %}
    -- Mở rộng buffer 14 ngày trước ngày max hiện tại trong fact table 
    -- để đảm bảo tính đúng LAG() cho các sản phẩm không xuất hiện liên tục mỗi ngày
    where snapshot_date >= (
        select date_sub(max(snapshot_date), interval 14 day) from {{ this }}
    )
    {% endif %}
),

with_lag as (
    select
        *,
        lag(price) over (
            partition by product_id
            order by snapshot_date
        ) as previous_day_price
    from history
),

with_price_change_date as (
    select
        *,
        -- Lấy ngày đổi giá gần nhất (hoặc ngày xuất hiện đầu tiên) tính đến snapshot_date hiện tại
        max(
            case 
                when price != previous_day_price or previous_day_price is null 
                then snapshot_date 
            end
        ) over (
            partition by product_id
            order by snapshot_date
            rows between unbounded preceding and current row
        ) as last_price_change_date
    from with_lag
)

select
    {{ dbt_utils.generate_surrogate_key(['w.product_id', 'w.snapshot_date']) }} as fact_key,
    w.product_id,
    w.snapshot_date,

    -- seller_id/brand_id lấy TRỰC TIẾP từ data của đúng ngày đó (không join qua dim_product)
    w.seller_id,
    w.brand_id,
    w.primary_category_name,

    w.price,
    w.original_price,
    w.discount_amount,
    w.discount_rate,
    w.availability,
    w.rating_average,
    w.review_count,
    w.quantity_sold,

    -- Đảm bảo giới hạn [0, 100]% để không fail dbt test khi dữ liệu thô bất thường
    round(
        greatest(0.0, least(100.0, safe_divide(w.original_price - w.price, w.original_price) * 100)),
        2
    ) as discount_pct,

    (w.original_price > w.price) as is_promotion,

    -- % thay đổi giá so với ngày crawl gần nhất trước đó
    round(safe_divide(w.price - w.previous_day_price, w.previous_day_price) * 100, 2) as price_change_pct,

    -- Số ngày giá đứng yên kể từ lần đổi giá gần nhất tính đến ngày snapshot này
    date_diff(w.snapshot_date, w.last_price_change_date, day) as days_since_last_price_change

from with_price_change_date w

{% if is_incremental() %}
-- Chỉ ghi nhận những ngày mới hơn ngày lớn nhất đã có trong fact table
where w.snapshot_date > (select max(snapshot_date) from {{ this }})
{% endif %}