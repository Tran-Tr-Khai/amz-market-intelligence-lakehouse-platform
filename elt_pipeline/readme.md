[ LOCAL STORAGE ]  ==> Cửa 1: Dagster Asset (Compute) ==> Cửa 2: IO Manager (Storage) ==> [ MINIO LAKEHOUSE ]
(Thư mục vật lý)       (File: bronze.py)                  (File: minio_io_manager.py)     (Định dạng Delta)

  📁 data/search/
       └── 📁 2026-03-04/
             ├── 📄 file1.json
             └── 📄 file2.json
                      │
                      ▼
            ┌──────────────────────────────────┐
            │ @asset: amazon_search_results    │
            │ 1. Đọc context lấy ngày tháng    │
            │ 2. Quét (Glob) tìm file JSON     │
            │ 3. Nạp vào Polars DataFrame      │
            │ 4. Làm phẳng và gắn nhãn data    │
            └──────────────────────────────────┘
                      │
                      ▼ (Truyền pl.DataFrame)
            ┌──────────────────────────────────┐
            │ class: MinioIOManager            │
            │ 1. Nhận DataFrame từ Asset       │
            │ 2. Sinh đường dẫn S3 động        │
            │ 3. Gọi hàm write_delta()         │
            │ 4. Tạo _delta_log và file Parquet│
            └──────────────────────────────────┘
                      │
                      ▼
  🪣 Bucket: s3://lakehouse/
       └── 📁 bronze/
            └── 📁 amazon/
                 └── 📁 search_results/
                      ├── 📁 _delta_log/       <-- (Nhật ký giao dịch)
                      ├── 📄 part-001.parquet  <-- (Dữ liệu thực tế)
                      └── 📄 part-002.parquet