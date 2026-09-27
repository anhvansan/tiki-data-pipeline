{% snapshot sellers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='seller_id',
        strategy='check',
        check_cols=['seller_name'],
        invalidate_hard_deletes=True,
    )
}}

-- SCD2 riêng cho seller — seller đổi tên là 1 sự kiện độc lập với sản phẩm,
-- không nên trộn vào products_snapshot (xem giải thích ở products_snapshot.sql).

select * from {{ ref('stg_sellers') }}

{% endsnapshot %}