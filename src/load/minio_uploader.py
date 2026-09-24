"""
minio_uploader.py
-------------------
Đẩy raw JSON đã crawl (ở data/raw/tiki/{date}/{category}/*.json) lên MinIO.
Giữ nguyên nội dung — đây vẫn là Bronze layer, KHÔNG transform ở đây.

Yêu cầu: MinIO đã chạy (docker compose up -d trong infrastructure/minio),
và đã tạo sẵn bucket (script này tự tạo bucket nếu chưa có).

Cách chạy:
    python -m src.load.minio_uploader --date 2026-09-23
    # hoặc không truyền --date để mặc định dùng hôm nay
"""

import argparse
import logging
import os
from datetime import date
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BUCKET_NAME = "tiki-raw"

MINIO_CONFIG = {
    "endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
    "aws_access_key_id": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
    "aws_secret_access_key": os.environ.get("MINIO_ROOT_PASSWORD", "minioadminpassword"),
}


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_CONFIG["endpoint_url"],
        aws_access_key_id=MINIO_CONFIG["aws_access_key_id"],
        aws_secret_access_key=MINIO_CONFIG["aws_secret_access_key"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket_name: str):
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info("Bucket '%s' đã tồn tại.", bucket_name)
    except ClientError:
        logger.info("Bucket '%s' chưa tồn tại, đang tạo...", bucket_name)
        client.create_bucket(Bucket=bucket_name)


def upload_date_folder(client, local_root: Path, run_date: str) -> dict:
    """Upload toàn bộ file JSON trong data/raw/tiki/{run_date}/ lên MinIO,
    giữ nguyên cấu trúc path -> key: raw/tiki/{run_date}/{category}/{file}.json
    """
    date_dir = local_root / run_date
    if not date_dir.exists():
        logger.error(
            "Không tìm thấy thư mục local: %s. Chạy crawler trước đã.", date_dir)
        return {"uploaded": 0, "failed": 0}

    summary = {"uploaded": 0, "failed": 0}

    json_files = list(date_dir.rglob("*.json"))
    if not json_files:
        logger.warning("Thư mục %s không có file JSON nào.", date_dir)
        return summary

    for file_path in json_files:
        # data/raw/tiki/2026-09-23/dien_thoai_may_tinh_bang/page_001.json
        # -> key: raw/tiki/2026-09-23/dien_thoai_may_tinh_bang/page_001.json
        relative = file_path.relative_to(local_root)
        key = f"raw/tiki/{relative.as_posix()}"

        try:
            client.upload_file(str(file_path), BUCKET_NAME, key)
            summary["uploaded"] += 1
            logger.info("Uploaded: %s", key)
        except Exception as e:
            summary["failed"] += 1
            logger.error("Lỗi upload %s: %s", file_path, e)

    return summary


def run(run_date: str):
    client = get_client()
    ensure_bucket(client, BUCKET_NAME)

    local_root = Path("data/raw/tiki")
    summary = upload_date_folder(client, local_root, run_date)

    logger.info(
        "=== TỔNG KẾT UPLOAD ngày %s: %s thành công, %s thất bại ===",
        run_date, summary["uploaded"], summary["failed"],
    )

    if summary["failed"] > 0:
        raise RuntimeError(
            f"{summary['failed']} file upload thất bại — kiểm tra log trước khi chạy bước tiếp theo."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="Ngày cần upload, format YYYY-MM-DD")
    args = parser.parse_args()
    run(args.date)
