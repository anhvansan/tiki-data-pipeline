"""
clean_products.py
-------------------
Giai đoạn 2 (phần Clean): đọc raw JSON từ MinIO, chuẩn hóa bằng pandas,
ghi lại cleaned JSON vào MinIO (path riêng), kèm data quality log.

Nguyên tắc: KHÔNG tự ý auto-fill giá trị mặc định vô căn cứ (VD: giá null
-> 0) — chỉ áp dụng các rule đã verify rõ ràng từ bước explore_api.py:
  - quantity_sold: object {"value": N} hoặc null -> lấy .value, fallback 0
  - availability: dùng làm cờ còn hàng (1) / hết hàng (0)
  - Loại field rác không cần cho phân tích (badges_*, impression_info...)

Cách chạy:
    python -m src.transform.clean_products --date 2026-09-23
"""

import argparse
import io
import json
import logging
from datetime import date

import pandas as pd

from src.load.minio_uploader import get_client, BUCKET_NAME

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Field thật sự cần cho fact/dim table — mọi field khác bị loại bỏ ở đây.
# Nếu sau này cần thêm field mới, sửa danh sách này, ko sửa rải rác trong code.
KEEP_FIELDS = [
    "id", "sku", "name", "url_key", "availability",
    "seller_id", "seller_name", "brand_id", "brand_name",
    "price", "original_price", "discount", "discount_rate",
    "rating_average", "review_count", "quantity_sold",
    "primary_category_name", "primary_category_path",
]

# thiếu 1 trong 3 field này -> loại record
REQUIRED_FIELDS = ["id", "price", "seller_id"]


def list_raw_files(client, run_date: str) -> list[str]:
    prefix = f"raw/tiki/{run_date}/"
    resp = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    return [obj["Key"] for obj in resp.get("Contents", [])]


def load_raw_json(client, key: str) -> list[dict]:
    obj = client.get_object(Bucket=BUCKET_NAME, Key=key)
    data = json.loads(obj["Body"].read())
    return data.get("data", [])


def extract_quantity_sold(value) -> int:
    if isinstance(value, dict):
        return value.get("value", 0) or 0
    return 0


def clean_dataframe(raw_items: list[dict], run_date: str) -> tuple[pd.DataFrame, dict]:
    """Trả về (df đã clean, quality_report)."""
    df = pd.DataFrame(raw_items)
    quality_report = {"input_rows": len(df)}

    if df.empty:
        quality_report["output_rows"] = 0
        return df, quality_report

    # Chỉ giữ field cần thiết (field không tồn tại trong response -> NaN)
    for col in KEEP_FIELDS:
        if col not in df.columns:
            df[col] = None
    df = df[KEEP_FIELDS].copy()

    # quantity_sold là object -> flatten về số
    df["quantity_sold"] = df["quantity_sold"].apply(extract_quantity_sold)

    # Loại record thiếu field bắt buộc
    before = len(df)
    df = df.dropna(subset=REQUIRED_FIELDS)
    quality_report["dropped_missing_required"] = before - len(df)

    # Loại giá trị giá phi lý (giá <= 0) — không auto-sửa, chỉ loại và log
    before = len(df)
    df = df[df["price"] > 0]
    quality_report["dropped_invalid_price"] = before - len(df)

    # Loại duplicate theo id (cùng sản phẩm xuất hiện 2 lần trong cùng ngày
    # do overlap giữa các page, hoặc bị trùng khi gọi lại do lỗi mạng)
    before = len(df)
    df = df.drop_duplicates(subset=["id"])
    quality_report["dropped_duplicate_id"] = before - len(df)

    # Thêm cột snapshot_date -> bắt buộc cho việc xây SCD2 sau này ở dbt
    df["snapshot_date"] = run_date

    quality_report["output_rows"] = len(df)
    return df, quality_report


def run(run_date: str):
    client = get_client()
    raw_keys = list_raw_files(client, run_date)

    if not raw_keys:
        logger.error(
            "Không tìm thấy raw file nào cho ngày %s trong MinIO.", run_date)
        return

    logger.info("Tìm thấy %s raw file cho ngày %s.", len(raw_keys), run_date)

    all_items = []
    for key in raw_keys:
        items = load_raw_json(client, key)
        all_items.extend(items)

    df, quality_report = clean_dataframe(all_items, run_date)

    logger.info("=== DATA QUALITY REPORT (%s) ===", run_date)
    for k, v in quality_report.items():
        logger.info("  %s: %s", k, v)

    if df.empty:
        logger.error(
            "DataFrame rỗng sau khi clean — KHÔNG ghi lên MinIO, kiểm tra lại raw data.")
        return

    # Ghi cleaned data lên MinIO dưới dạng JSON lines, path riêng khỏi raw/
    buffer = io.StringIO()
    df.to_json(buffer, orient="records", lines=True, force_ascii=False)
    cleaned_key = f"cleaned/tiki/{run_date}/products.jsonl"
    client.put_object(Bucket=BUCKET_NAME, Key=cleaned_key,
                      Body=buffer.getvalue().encode("utf-8"))

    logger.info("Đã ghi %s dòng cleaned data vào MinIO: %s",
                len(df), cleaned_key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    run(args.date)
