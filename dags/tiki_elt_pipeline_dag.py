"""
tiki_elt_pipeline_dag.py
-------------------------
DAG chính cho pipeline Tiki Price & Promotion Intelligence.

Hiện tại:
    crawl_tiki_api >> upload_raw_to_minio

Các task phía sau sẽ được thêm từng bước:
    clean_products
    load_to_bigquery
    dbt_run
    dbt_test

Nguyên tắc:
- DAG chỉ làm orchestration.
- Business/data processing logic nằm trong src/.
- Các task dùng cùng run_date từ Airflow để hỗ trợ retry/re-run/backfill.
"""

from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

from src.extract.tiki_api_crawler import run as run_crawler
from src.load.minio_uploader import run as run_minio_upload
from src.transform.clean_products import run as run_clean_products


def crawl_tiki_api_task(**context):
    """Chạy crawler theo ngày của DAG run."""
    run_date = context["ds"]
    run_crawler(run_date)


def upload_raw_to_minio_task(**context):
    """Upload raw data tương ứng với ngày của DAG run."""
    run_date = context["ds"]
    run_minio_upload(run_date)


def clean_products_task(**context):
    run_date = context["ds"]
    run_clean_products(run_date)


default_args = {
    "owner": "jaimee",
    "retries": 1,
}

with DAG(
    dag_id="tiki_elt_pipeline",
    description=(
        "Crawl gia va khuyen mai san pham Tiki, day len MinIO, "
        "clean, load BigQuery, transform bang dbt"
    ),
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["tiki", "elt"],
) as dag:

    crawl_tiki_api = PythonOperator(
        task_id="crawl_tiki_api",
        python_callable=crawl_tiki_api_task,
    )

    upload_raw_to_minio = PythonOperator(
        task_id="upload_raw_to_minio",
        python_callable=upload_raw_to_minio_task,
    )

    clean_products = PythonOperator(
        task_id="clean_products",
        python_callable=clean_products_task,
    )

    crawl_tiki_api >> upload_raw_to_minio >> clean_products
