# Kiến trúc hệ thống

## 1. Kiến trúc Microservices / SOA

Hệ thống được chia thành các service độc lập theo đúng tinh thần SOA:

- `ingestion-service`: thu thập dữ liệu cổ phiếu thật từ Yahoo Finance và lưu `parquet`.
- `etl-service`: chạy trên Spark/Databricks để làm sạch, tạo feature và ghi vào `Delta Lake`.
- `model-service`: huấn luyện nhiều mô hình ML, đánh giá RMSE, chọn mô hình tốt nhất và sinh `prediction`.
- `api-service`: cung cấp endpoint REST cho việc truy vấn dữ liệu và dự đoán.
- `dashboard-service`: hiển thị trực quan dữ liệu thực tế và dự đoán bằng Plotly/Dash.

## 2. Logic Diagram

```text
                    +----------------------+
                    |   Yahoo Finance API  |
                    +----------+-----------+
                               |
                               v
                   +-----------+------------+
                   |   Ingestion Service    |
                   | yfinance -> parquet    |
                   +-----------+------------+
                               |
                               v
                   +-----------+------------+
                   |      ETL Service       |
                   | Spark + Feature Eng.   |
                   +-----------+------------+
                               |
                               v
         +---------------------+----------------------+
         |                                            |
         v                                            v
+--------+---------+                         +--------+---------+
|  Delta Feature   |                         |  Processed Files |
|      Table       |                         | parquet/artifacts|
+--------+---------+                         +--------+---------+
         |                                            |
         v                                            v
+--------+---------+                         +--------+---------+
|   Model Service  |------------------------>| Dashboard Service |
| LR / RF / GBT    |                         | Plotly / Dash     |
+--------+---------+                         +--------+---------+
         |
         v
+--------+---------+
| Prediction Delta |
| + model artifacts|
+--------+---------+
         |
         v
+--------+---------+
|    API Service   |
| /predict /data   |
+------------------+
```

## 3. Luồng xử lý

1. Ingestion kéo dữ liệu giá nhiều mã cổ phiếu `AAPL`, `TSLA`, `MSFT`.
2. ETL chuẩn hóa schema và sinh đặc trưng `MA10`, `MA20`, `lag`, `RSI`, `daily_return`.
3. Model service train `Linear Regression`, `Random Forest`, `GBTRegressor`.
4. Chọn model tốt nhất theo `RMSE`.
5. Sinh bảng dự đoán cho API và dashboard.
6. Databricks Jobs orchestration tự động hóa toàn bộ pipeline theo lịch hàng ngày.

## 4. Vì sao thiết kế này phù hợp đồ án 9-10 điểm

- Phân tách service rõ ràng, dễ mở rộng.
- Có `Delta Lake`, `Spark ML`, `Databricks Jobs`, `Notebook`, `API`, `Dashboard`.
- Hỗ trợ multi-stock thật, dữ liệu thật, pipeline thật.
- Có artifact, config, workflow và tài liệu triển khai.

