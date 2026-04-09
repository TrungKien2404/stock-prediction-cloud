# Cloud Stock Analytics on Databricks

Hệ thống phân tích và dự đoán giá cổ phiếu theo hướng `Microservices / SOA`, được thiết kế để chạy trên `Databricks + Spark + Delta Lake` với dữ liệu thật từ Yahoo Finance.

## Mục tiêu đồ án

- Thu thập dữ liệu nhiều mã cổ phiếu hằng ngày.
- Làm sạch và xây dựng feature bằng Spark.
- Huấn luyện nhiều mô hình ML, so sánh RMSE, chọn mô hình tốt nhất.
- Phục vụ kết quả qua FastAPI và dashboard Plotly.
- Tự động hóa pipeline bằng Databricks Jobs.

## Cấu trúc project

```text
cloud2/
├── api/                  # FastAPI service
├── common/               # Shared config, logging, Spark helpers
├── configs/              # YAML configuration
├── dashboard/            # Plotly/Dash dashboard + HTML export
├── docs/                 # Architecture and Databricks guides
├── etl/                  # Spark ETL + feature engineering
├── ingestion/            # Yahoo Finance ingestion service
├── jobs/                 # Databricks workflow JSON
├── model/                # Spark ML training and model selection
├── notebooks/            # Databricks notebooks
├── scripts/              # Local orchestration script
├── data/                 # Raw, processed, delta, artifacts
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Thành phần hệ thống

### 1. Ingestion service

- Nguồn: `Yahoo Finance` qua `yfinance`
- Hỗ trợ multi-stock: `AAPL`, `TSLA`, `MSFT`
- Lưu raw data dưới dạng `parquet`

### 2. ETL service

- Đọc raw parquet bằng Spark
- Làm sạch dữ liệu, chuẩn hóa schema
- Tạo feature:
  - `MA10`, `MA20`
  - `lag_1`, `lag_2`, `lag_3`
  - `RSI(14)`
  - `daily_return`
- Ghi dữ liệu vào `Delta Lake`

### 3. Model service

- Train 3 model:
  - `Linear Regression`
  - `Random Forest Regressor`
  - `GBT Regressor`
- Đánh giá bằng `RMSE`
- Chọn best model
- Lưu:
  - model artifact
  - metrics
  - prediction parquet
  - prediction Delta table

### 4. API service

- `GET /predict/{ticker}`: trả dự đoán mới nhất cho mã cổ phiếu
- `GET /data`: trả dữ liệu feature đã xử lý

### 5. Dashboard service

- Dashboard tương tác với `Plotly + Dash`
- File HTML được export để lưu artifact từ Databricks Job
- Hiển thị:
  - Actual vs Predicted
  - Multi-stock comparison

## Quy trình pipeline

```text
Yahoo Finance
  -> ingestion/
  -> parquet raw
  -> etl/
  -> Delta Lake
  -> model/
  -> predictions + metrics + model artifact
  -> api/ and dashboard/
```

## Chạy local

### 1. Cài dependencies

```bash
pip install -r requirements.txt
```

### 2. Chạy pipeline end-to-end

```bash
python scripts/run_local_pipeline.py
```

### 3. Chạy API

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

### 4. Chạy dashboard

```bash
python dashboard/app.py
```

## Chạy trên Databricks

### Cách 1. Free Edition

- Dùng notebook [99_free_tier_end_to_end.py](/e:/Kien_HK2_Nam3/cloud2/notebooks/99_free_tier_end_to_end.py)
- Chỉnh `project_root`, `config_path`, `base_path`
- Chạy từng cell

### Cách 2. Databricks Jobs

- Import file `jobs/databricks_workflow.json`
- Workflow gồm:
  1. ingestion
  2. etl
  3. train
  4. dashboard
- Có thể schedule chạy hàng ngày

## File quan trọng cần nắm

- Cấu hình chung: [settings.yaml](/e:/Kien_HK2_Nam3/cloud2/configs/settings.yaml)
- Ingestion pipeline: [pipeline.py](/e:/Kien_HK2_Nam3/cloud2/ingestion/pipeline.py)
- ETL pipeline: [pipeline.py](/e:/Kien_HK2_Nam3/cloud2/etl/pipeline.py)
- ML training: [train.py](/e:/Kien_HK2_Nam3/cloud2/model/train.py)
- API: [main.py](/e:/Kien_HK2_Nam3/cloud2/api/main.py)
- Dashboard export: [report.py](/e:/Kien_HK2_Nam3/cloud2/dashboard/report.py)
- Kiến trúc: [architecture.md](/e:/Kien_HK2_Nam3/cloud2/docs/architecture.md)
- Hướng dẫn Databricks: [databricks_setup.md](/e:/Kien_HK2_Nam3/cloud2/docs/databricks_setup.md)

## Điểm nổi bật để bảo vệ đồ án

- Kiến trúc `Microservices / SOA`
- Dữ liệu thật, cập nhật hằng ngày
- `Spark + Delta Lake + Databricks Notebook + Jobs`
- `RMSE-based model selection`
- `FastAPI + Plotly Dashboard`
- Multi-stock processing, feature engineering nâng cao, code cấu trúc rõ ràng

