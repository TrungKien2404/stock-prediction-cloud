# Hướng dẫn chạy trên Databricks

## 1. Chuẩn bị repo

1. Tạo Databricks Repo và sync source code này vào workspace.
2. Mở notebook `notebooks/99_free_tier_end_to_end.py` nếu dùng Databricks Free Edition.
3. Nếu dùng Databricks Jobs bản đầy đủ, import file `jobs/databricks_workflow.json`.

## 2. Cấu hình thư mục lưu trữ

- `project_root`: `/Workspace/Repos/<your-user>/cloud2`
- `config_path`: `/Workspace/Repos/<your-user>/cloud2/configs/settings.yaml`
- `base_path`: `/dbfs/FileStore/cloud_stock_analytics`

`base_path` là nơi tất cả parquet, delta và artifact sẽ được lưu trên DBFS.

## 3. Chạy từng bước

### Notebook ingestion

- Mở `notebooks/01_ingestion_databricks.py`
- Chạy từng cell để lấy dữ liệu thật từ Yahoo Finance

### Notebook ETL

- Mở `notebooks/02_etl_databricks.py`
- Chạy để làm sạch dữ liệu và ghi vào Delta Lake

### Notebook Train

- Mở `notebooks/03_train_databricks.py`
- Chạy để train 3 mô hình và chọn mô hình tốt nhất

### Notebook Dashboard

- Mở `notebooks/04_dashboard_databricks.py`
- Chạy để sinh file dashboard HTML

## 4. Dùng Databricks Free Edition

Nếu không tạo được Job workflow tự động, dùng notebook:

- `notebooks/99_free_tier_end_to_end.py`

Notebook này đã gom toàn bộ các bước thành các cell riêng để bạn copy trực tiếp khi bảo vệ hoặc demo.

## 5. Schedule tự động

Với workspace hỗ trợ Jobs:

1. Import `jobs/databricks_workflow.json`
2. Chỉnh lại `notebook_path`, `project_root`, `config_path`
3. Điều chỉnh `node_type_id` theo cluster của bạn
4. Bật schedule hằng ngày lúc `07:00 Asia/Bangkok`

## 6. Kết quả đầu ra

- Raw parquet: `data/raw/stock_prices`
- Processed parquet: `data/processed/stock_features`
- Delta feature table: `stock_analytics.stock_features`
- Delta prediction table: `stock_analytics.stock_predictions`
- Metrics: `data/artifacts/metrics`
- Dashboard HTML: `data/artifacts/dashboard/stock_dashboard.html`

