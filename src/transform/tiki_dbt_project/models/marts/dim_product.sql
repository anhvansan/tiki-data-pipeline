-- dim_product.sql
-- Dimension SCD2 cho product — chỉ thuộc tính mô tả của BẢN THÂN sản phẩm
-- Giá, rating, quantity_sold KHÔNG ở đây — xem fact_product_snapshot.

with snapshot as (
    select * from {{ ref('products_snapshot') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }} as product_key,
    product_id,
    product_sku,
    product_name,
    product_url_key,
    brand_name,
    seller_id,
    primary_category_name,
    primary_category_path,
    dbt_valid_from                                     as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31'))     as valid_to,
    dbt_valid_to is null                                as is_current
from snapshot