with source as (
    select * from {{ ref('stg_products_history') }}
),

deduped as (
    select
        seller_id,
        nullif(trim(seller_name), '') as seller_name
    from source
    where seller_id is not null
    qualify row_number() over (
        partition by seller_id
        order by snapshot_date desc, seller_name
    ) = 1
)

select * from deduped