"""
crawler_config.py
------------------
Cấu hình crawl đã chốt sau khi verify trực tiếp trên API thật của Tiki
(endpoint /api/personalish/v1/blocks/listings, xem lịch sử test trong docs/).

KHÔNG hardcode con số này vào logic crawler — mọi thay đổi scope (thêm
category, đổi limit...) chỉ cần sửa file này.
"""

CRAWLER_CONFIG = {
    "base_url": "https://tiki.vn/api/personalish/v1/blocks/listings",
    "headers": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    },
    # Param cố định dùng chung cho mọi request — chỉ giữ những param đã
    # verify là bắt buộc/ổn định. Không đưa aggregations/include/version
    # vào vì đã xác nhận đây là param cá nhân hóa UI, không cần cho crawl.
    "common_params": {
        "limit": 40,
        "sort": "top_seller",
    },
    "delay_between_requests_sec": 1.5,
    "max_retries": 3,
    "retry_backoff_sec": 3,
    "request_timeout_sec": 10,
    "target_products_per_category": 150,
    "categories": [
        {
            "category_id": 1815,
            "url_key": "thiet-bi-kts-phu-kien-so",
            "folder_name": "thiet_bi_so_phu_kien",
            "known_total": 2000,  # verify 2026-09: total thật "10000+", API cap ở 2000
        },
        {
            "category_id": 1789,
            "url_key": "dien-thoai-may-tinh-bang",
            "folder_name": "dien_thoai_may_tinh_bang",
            "known_total": 107,  # verify 2026-09: category nhỏ, lấy hết 100%
        },
        {
            "category_id": 1846,
            "url_key": "laptop-may-vi-tinh-linh-kien",
            "folder_name": "laptop_may_vi_tinh",
            "known_total": 2000,
        },
    ],
}
