# Databricks Free Tier Cells

Tài liệu này tách sẵn code theo từng cell để bạn copy trực tiếp vào notebook mới trên Databricks Free Edition.

## Cell 1: Cài thư viện

```python
%pip install -r /Workspace/Repos/<your-user>/cloud2/requirements.txt
```

## Cell 2: Khai báo widget

```python
dbutils.widgets.text("project_root", "/Workspace/Repos/<your-user>/cloud2")
dbutils.widgets.text("config_path", "/Workspace/Repos/<your-user>/cloud2/configs/settings.yaml")
dbutils.widgets.text("base_path", "/dbfs/FileStore/cloud_stock_analytics")
```

## Cell 3: Import module

```python
import sys

project_root = dbutils.widgets.get("project_root")
config_path = dbutils.widgets.get("config_path")
base_path = dbutils.widgets.get("base_path")

if project_root not in sys.path:
    sys.path.append(project_root)

from dashboard.report import export_dashboard_html
from etl.pipeline import run_etl_pipeline
from ingestion.pipeline import run_ingestion_pipeline
from model.train import run_training_pipeline
```

## Cell 4: Ingestion

```python
ingestion_summary = run_ingestion_pipeline(config_path=config_path, base_path=base_path)
display(ingestion_summary)
```

## Cell 5: ETL

```python
etl_summary = run_etl_pipeline(config_path=config_path, base_path=base_path)
display(etl_summary)
```

## Cell 6: Train model

```python
training_summary = run_training_pipeline(config_path=config_path, base_path=base_path)
display(training_summary)
```

## Cell 7: Export dashboard

```python
dashboard_html_path = export_dashboard_html(config_path=config_path, base_path=base_path)
display({"dashboard_html_path": dashboard_html_path})
```

## Gợi ý khi demo

- Chạy lần lượt từ Cell 1 đến Cell 7.
- Sau khi xong, mở DBFS path được trả về để lấy file HTML dashboard.
- Dùng `SELECT * FROM stock_analytics.stock_features` để kiểm tra Delta table.
- Dùng `SELECT * FROM stock_analytics.stock_predictions` để chứng minh pipeline ML đã ghi kết quả.

