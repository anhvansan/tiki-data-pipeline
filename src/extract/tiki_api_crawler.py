"""
tiki_api_crawler.py
--------------------
Crawler chính thức cho Giai đoạn 1 (Extract) của pipeline Tiki Price
& Promotion Intelligence.

Nguyên tắc: script này CHỈ làm 1 việc — gọi API, lưu RAW JSON nguyên bản
xuống local (sau này load.minio_uploader sẽ đẩy lên MinIO). Không làm
transform/clean ở đây (giữ đúng nguyên tắc Bronze layer = không sửa data).

Cách chạy:
    python -m src.extract.tiki_api_crawler
"""

import json
import logging
import time
from datetime import date
from pathlib import Path

import requests

from src.config.crawler_config import CRAWLER_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_page(category: dict, page: int) -> dict | None:
    """Gọi 1 page của 1 category, có retry khi lỗi tạm thời (timeout, 5xx)."""
    params = {
        **CRAWLER_CONFIG["common_params"],
        "category": category["category_id"],
        "urlKey": category["url_key"],
        "page": page,
    }

    last_error = None
    for attempt in range(1, CRAWLER_CONFIG["max_retries"] + 1):
        try:
            resp = requests.get(
                CRAWLER_CONFIG["base_url"],
                params=params,
                headers=CRAWLER_CONFIG["headers"],
                timeout=CRAWLER_CONFIG["request_timeout_sec"],
            )

            if resp.status_code == 200:
                return resp.json()

            # 429 = rate limit, 5xx = lỗi server tạm thời -> đáng để retry
            if resp.status_code == 429 or resp.status_code >= 500:
                logger.warning(
                    "Category %s page %s: status %s, thử lại (%s/%s)",
                    category["category_id"], page, resp.status_code,
                    attempt, CRAWLER_CONFIG["max_retries"],
                )
                last_error = f"HTTP {resp.status_code}"
                time.sleep(CRAWLER_CONFIG["retry_backoff_sec"] * attempt)
                continue

            # 4xx khác (400, 404...) -> lỗi do request sai, retry không ích gì
            logger.error(
                "Category %s page %s: status %s (không retry) - %s",
                category["category_id"], page, resp.status_code, resp.text[:200],
            )
            return None

        except requests.RequestException as e:
            last_error = str(e)
            logger.warning(
                "Category %s page %s: lỗi kết nối '%s', thử lại (%s/%s)",
                category["category_id"], page, e, attempt, CRAWLER_CONFIG["max_retries"],
            )
            time.sleep(CRAWLER_CONFIG["retry_backoff_sec"] * attempt)

    logger.error(
        "Category %s page %s: THẤT BẠI sau %s lần thử. Lỗi cuối: %s",
        category["category_id"], page, CRAWLER_CONFIG["max_retries"], last_error,
    )
    return None


def crawl_category(category: dict, run_date: str, out_root: Path) -> dict:
    """Crawl đủ số trang cần thiết để đạt target_products_per_category,
    lưu từng page thành 1 file JSON raw riêng biệt.
    Trả về summary (số page thành công/thất bại) để log cuối cùng.
    """
    target = CRAWLER_CONFIG["target_products_per_category"]
    limit = CRAWLER_CONFIG["common_params"]["limit"]
    max_pages_needed = -(-target // limit)  # ceil division

    out_dir = out_root / run_date / category["folder_name"]
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {"category_id": category["category_id"],
               "ok_pages": 0, "failed_pages": 0, "total_items": 0}

    for page in range(1, max_pages_needed + 1):
        data = fetch_page(category, page)

        if data is None:
            summary["failed_pages"] += 1
            continue

        items = data.get("data", [])
        summary["ok_pages"] += 1
        summary["total_items"] += len(items)

        out_path = out_dir / f"page_{page:03d}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # Nếu page trả về ít hơn limit hoặc đã hết last_page -> dừng sớm,
        # không cần gọi thêm page thừa.
        paging = data.get("paging", {})
        last_page = paging.get("last_page")
        if not items or (last_page and page >= last_page):
            logger.info(
                "Category %s: hết dữ liệu ở page %s (last_page=%s), dừng sớm.",
                category["category_id"], page, last_page,
            )
            break

        time.sleep(CRAWLER_CONFIG["delay_between_requests_sec"])

    return summary


def run():
    run_date = date.today().isoformat()
    out_root = Path("data/raw/tiki")

    logger.info("Bắt đầu crawl ngày %s cho %s category",
                run_date, len(CRAWLER_CONFIG["categories"]))

    all_summaries = []
    for category in CRAWLER_CONFIG["categories"]:
        logger.info("--- Crawl category %s (%s) ---",
                    category["category_id"], category["folder_name"])
        summary = crawl_category(category, run_date, out_root)
        all_summaries.append(summary)
        time.sleep(CRAWLER_CONFIG["delay_between_requests_sec"])

    logger.info("=== TỔNG KẾT CRAWL NGÀY %s ===", run_date)
    for s in all_summaries:
        logger.info(
            "Category %s: %s page OK, %s page FAIL, %s sản phẩm",
            s["category_id"], s["ok_pages"], s["failed_pages"], s["total_items"],
        )

    total_failed = sum(s["failed_pages"] for s in all_summaries)
    if total_failed > 0:
        logger.warning(
            "Có %s page thất bại — kiểm tra lại data/raw/tiki/%s trước khi chạy bước Load.",
            total_failed, run_date,
        )


if __name__ == "__main__":
    run()
