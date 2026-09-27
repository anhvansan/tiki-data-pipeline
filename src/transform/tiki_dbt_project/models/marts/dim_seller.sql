-- dim_seller.sql
with snapshot as (
    select * from {{ ref('sellers_snapshot') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['seller_id', 'dbt_valid_from']) }} as seller_key,
    seller_id,
    seller_name,
    dbt_valid_from                                     as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31'))     as valid_to,
    dbt_valid_to is null                                as is_current
from snapshot