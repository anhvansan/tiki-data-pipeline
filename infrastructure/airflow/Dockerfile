# Base image chính thức của Airflow — pin version cụ thể, đừng dùng "latest"
# để tránh pipeline vỡ khi Airflow ra bản mới đột ngột.
FROM apache/airflow:2.10.2-python3.11

# Copy requirements riêng của project (boto3, pandas, dbt...) — tách khỏi
# requirements gốc của Airflow để dễ maintain.
COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt