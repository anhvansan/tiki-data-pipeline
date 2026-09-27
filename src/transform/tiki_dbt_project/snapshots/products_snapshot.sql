{% snapshot products_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'product_sku',
            'product_name',
            'product_url_key',
            'seller_id',
            'brand_id',
            'primary_category_name',
            'primary_category_path',
        ],
        invalidate_hard_deletes=True,
    )
}}

select * from {{ ref('stg_products') }}

{% endsnapshot %}