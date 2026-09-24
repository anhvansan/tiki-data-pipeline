"""
bigquery_loader.py
--------------------
Giai đoạn 2 (bước cuối): đọc cleaned JSONL từ MinIO, load vào BigQuery
bảng staging.products. Dùng WRITE_TRUNCATE cho snapshot ngày đó (staging
chỉ chứa batch mới nhất) — dữ liệu lịch sử thật sự được giữ lại ở tầng
dbt snapshot (SCD2), không phải ở staging.

Yêu cầu trước khi chạy:
  - Đã tạo GCP service account, bật BigQuery API (xem hướng dẫn kèm theo).
  - Đã đặt file key JSON vào src/config/gcp_service_account.json
  - Đã tạo dataset "staging" trong BigQuery (script này tự tạo nếu chưa có)

Cách chạy:
    python -m src.load.bigquery_loader --date 2026-09-23
"""

import argparse
import io
import json
import logging
import os
from datetime import date

from google.cloud import bigquery
from google.oauth2 import service_account

from src.load.minio_uploader import get_client, BUCKET_NAME

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATASET_ID = "staging"
TABLE_ID = "products"

# Schema khai báo tường minh — KHÔNG dùng autodetect, để tránh BigQuery
# tự đoán sai kiểu dữ liệu (VD: đoán price là INTEGER rồi sau này gặp
# giá trị lẻ lại lỗi). Khai báo rõ ràng = pipeline ổn định hơn khi scale.
SCHEMA = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("url_key", "STRING"),
    bigquery.SchemaField("availability", "INTEGER"),
    bigquery.SchemaField("seller_id", "INTEGER"),
    bigquery.SchemaField("seller_name", "STRING"),
    bigquery.SchemaField("brand_id", "INTEGER"),
    bigquery.SchemaField("brand_name", "STRING"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("original_price", "FLOAT"),
    bigquery.SchemaField("discount", "FLOAT"),
    bigquery.SchemaField("discount_rate", "FLOAT"),
    bigquery.SchemaField("rating_average", "FLOAT"),
    bigquery.SchemaField("review_count", "INTEGER"),
    bigquery.SchemaField("quantity_sold", "INTEGER"),
    bigquery.SchemaField("primary_category_name", "STRING"),
    bigquery.SchemaField("primary_category_path", "STRING"),
    bigquery.SchemaField("snapshot_date", "DATE"),
]


def get_bq_client() -> bigquery.Client:
    key_path = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "src/config/gcp_service_account.json",
    )
    project_id = os.environ.get("GCP_PROJECT_ID")

    if not os.path.exists(key_path):
        raise FileNotFoundError(
            f"Không tìm thấy service account key tại '{key_path}'. "
            "Xem hướng dẫn tạo key ở phần chú thích đầu file."
        )

    credentials = service_account.Credentials.from_service_account_file(
        key_path)
    return bigquery.Client(credentials=credentials, project=project_id or credentials.project_id)


def ensure_dataset(client: bigquery.Client):
    dataset_ref = f"{client.project}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset '%s' đã tồn tại.", dataset_ref)
    except Exception:
        logger.info("Dataset '%s' chưa tồn tại, đang tạo...", dataset_ref)
        dataset = bigquery.Dataset(dataset_ref)
        # gần VN, giảm latency + đúng khu vực dữ liệu
        dataset.location = "asia-southeast1"
        client.create_dataset(dataset)


def ensure_table(client: bigquery.Client, table_ref: str):
    """Tạo table với partition theo snapshot_date nếu chưa tồn tại.
    Partition theo ngày là điều kiện để dùng WRITE_TRUNCATE trên 1 partition
    cụ thể (thay cho DELETE DML — DML bị chặn khi project chưa bật billing).
    """
    try:
        client.get_table(table_ref)
        logger.info("Table '%s' đã tồn tại.", table_ref)
    except Exception:
        logger.info(
            "Table '%s' chưa tồn tại, đang tạo (partitioned theo snapshot_date)...", table_ref)
        table = bigquery.Table(table_ref, schema=SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="snapshot_date",
        )
        client.create_table(table)


def read_cleaned_jsonl(run_date: str) -> list[dict]:
    minio_client = get_client()
    key = f"cleaned/tiki/{run_date}/products.jsonl"

    try:
        obj = minio_client.get_object(Bucket=BUCKET_NAME, Key=key)
    except minio_client.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"Không tìm thấy '{key}' trên MinIO — chạy clean_products.py trước."
        )

    content = obj["Body"].read().decode("utf-8")
    rows = [json.loads(line) for line in content.strip().split("\n") if line]
    return rows


def load_to_bigquery(client: bigquery.Client, rows: list[dict], run_date: str):
    table_ref = f"{client.project}.{DATASET_ID}.{TABLE_ID}"
    ensure_table(client, table_ref)

    # Load thẳng vào PARTITION của đúng ngày đó (decorator "$YYYYMMDD"), dùng
    # WRITE_TRUNCATE. Vì đây là Load API (không phải DML), nên:
    #   - Chạy được cả khi project chưa bật billing (tránh lỗi billingNotEnabled)
    #   - WRITE_TRUNCATE chỉ xóa/ghi đè đúng partition đó, các ngày khác không
    #     bị ảnh hưởng -> an toàn khi chạy lại nhiều lần cho cùng 1 ngày.
    partition_suffix = run_date.replace("-", "")  # "2026-09-23" -> "20260923"
    partitioned_table_ref = f"{table_ref}${partition_suffix}"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    jsonl_data = "\n".join(json.dumps(r, ensure_ascii=False)
                           for r in rows).encode("utf-8")
    buffer = io.BytesIO(jsonl_data)
    job = client.load_table_from_file(
        buffer, partitioned_table_ref, job_config=job_config)
    job.result()  # đợi job hoàn tất, raise lỗi nếu load thất bại

    logger.info("Đã load (ghi đè) %s dòng vào partition %s",
                len(rows), partitioned_table_ref)


def run(run_date: str):
    client = get_bq_client()
    ensure_dataset(client)

    rows = read_cleaned_jsonl(run_date)
    logger.info("Đọc được %s dòng cleaned data từ MinIO cho ngày %s.",
                len(rows), run_date)

    if not rows:
        logger.error(
            "Không có dòng nào để load — dừng lại, không tạo job rỗng.")
        return

    load_to_bigquery(client, rows, run_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    run(args.date)
