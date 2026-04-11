# Query Engine Design — Lakehouse to Application Layer

> **Phạm vi:** Lớp SQL Engine nằm giữa MinIO (Delta Lake) và tầng Application (Superset + FastAPI).

---

## 1. Kết luận nhanh (TL;DR)

**Chọn DuckDB cho dự án này.**

Trino không sai về mặt kiến trúc — nhưng sai về mặt **cost/value** cho một dự án ở quy mô này. Phần còn lại của tài liệu giải thích tại sao.

---

## 2. Bảng so sánh

| Tiêu chí | Trino | DuckDB |
|---|---|---|
| **Loại** | Distributed query engine (server) | Embedded in-process OLAP engine |
| **Độ phức tạp cài đặt** | Cao — coordinator + metastore + catalog config | Thấp — cài pip package, load 2 extension |
| **RAM tối thiểu (Docker)** | ~2–4 GB (coordinator + 1 worker) | ~50 MB (in-process, không thêm container) |
| **Đọc Delta Lake từ MinIO** | Có — qua Delta connector + Hive Metastore | Có — qua `delta` + `httpfs` extension |
| **Superset integration** | Tốt nhất (official Trino dialect) | Tốt — qua `duckdb_engine` SQLAlchemy driver |
| **FastAPI integration** | Cần gọi HTTP → Trino server | Gọi trực tiếp `import duckdb` |
| **Phù hợp data scale dự án này** | Overkill | Vừa đủ |
| **Khi nào nên migrate sang Trino** | Data > hàng trăm GB, nhiều user concurrent | — |

---

## 3. Tại sao không chọn Trino ngay lúc này

Để Trino đọc được Delta Lake trên MinIO, cần **thêm 3 thành phần**:

```
Trino coordinator (container ~1.5GB RAM)
  + Trino worker(s) (container ~1GB RAM mỗi worker)
  + Hive Metastore Service (HMS) — để Trino biết schema của Delta table
    + một PostgreSQL/MySQL làm backend cho HMS
```

Docker Compose hiện tại đã có MinIO + Spark master + Spark worker. Thêm Trino đồng nghĩa:
- 3–4 container mới
- HMS config phức tạp (catalog properties, schema registration)
- Debug overhead không tỷ lệ với data volume hiện tại (vài triệu rows)

**Trino là lựa chọn đúng khi:** team lớn, data hàng trăm GB+, nhiều analytical user query đồng thời.

---

## 4. Tại sao DuckDB phù hợp

DuckDB là một OLAP engine chạy **in-process** — không cần server riêng. Nó có thể:

1. Đọc thẳng **Delta Lake** từ MinIO qua S3-compatible API
2. Chạy bên trong **FastAPI** như một thư viện Python bình thường
3. Expose schema cho **Superset** qua SQLAlchemy driver `duckdb_engine`
4. Xử lý tốt vài chục triệu rows trên một máy đơn

Vì DuckDB đọc trực tiếp Parquet/Delta files trên MinIO (không copy data), cả FastAPI lẫn Superset đều có thể mở connection riêng của mình trỏ tới cùng một MinIO endpoint — **không conflict** vì đây là read-only workload.

---

## 5. Kiến trúc sau khi thêm DuckDB

```
┌──────────────────────────────────────────────────────────────────┐
│  MinIO (Object Storage)                                          │
│  s3a://lakehouse/gold/amazon/                                    │
│    ├── dim_product/           (Delta Lake)                       │
│    ├── fact_search_ranking/   (Delta Lake, partitioned)          │
│    ├── fact_price_snapshot/   (Delta Lake, partitioned)          │
│    ├── fact_product_performance/ (Delta Lake, partitioned)       │
│    ├── mart_keyword_daily/    (Delta Lake, partitioned)          │
│    └── mart_brand_competitive/ (Delta Lake, partitioned)         │
└───────────────────────┬──────────────────────────────────────────┘
                        │  S3-compatible reads (httpfs + delta ext)
           ┌────────────┴────────────┐
           │                         │
    ┌──────▼──────┐           ┌──────▼──────┐
    │  FastAPI    │           │  Superset   │
    │  (DuckDB    │           │  (DuckDB    │
    │  embedded)  │           │  via        │
    │             │           │  duckdb_    │
    │  /api/...   │           │  engine)    │
    └─────────────┘           └─────────────┘
```

Mỗi service mang DuckDB của riêng mình (in-process). Cả hai đều đọc cùng một set file trên MinIO. DuckDB không có kết nối server/port — nó là thư viện, không phải daemon.

---

## 6. Cách DuckDB kết nối MinIO + Delta Lake

```python
import duckdb

con = duckdb.connect()

# Cài extensions (chỉ cần làm một lần)
con.execute("INSTALL delta;")
con.execute("INSTALL httpfs;")
con.execute("LOAD delta;")
con.execute("LOAD httpfs;")

# Cấu hình S3/MinIO
con.execute("""
    CREATE OR REPLACE SECRET minio_secret (
        TYPE s3,
        KEY_ID 'tntkhai',
        SECRET '14442002',
        ENDPOINT 'localhost:9000',
        USE_SSL false,
        URL_STYLE 'path'
    );
""")

# Query Gold layer trực tiếp
df = con.execute("""
    SELECT *
    FROM delta_scan('s3://lakehouse/gold/amazon/mart_keyword_daily/')
    WHERE ingested_at = '2026-03-04'
""").df()
```

> **Lưu ý:** Dùng `SECRET` thay vì `SET s3_*` — đây là cách DuckDB 1.x khuyên dùng, tránh deprecated settings.

---

## 7. Tích hợp FastAPI

### 7.1 Cấu trúc

```
app/
├── main.py
├── db/
│   ├── __init__.py
│   └── duckdb_session.py    ← connection factory
└── routes/
    ├── keywords.py
    └── products.py
```

### 7.2 Connection factory (thread-safe)

DuckDB hỗ trợ multiple readers nhưng mỗi thread nên dùng connection riêng:

```python
# db/duckdb_session.py
import threading
import duckdb
from functools import lru_cache

_local = threading.local()

MINIO_ENDPOINT = "localhost:9000"
MINIO_KEY = "tntkhai"
MINIO_SECRET = "14442002"


def get_con() -> duckdb.DuckDBPyConnection:
    """Returns a per-thread DuckDB connection with MinIO configured."""
    if not hasattr(_local, "con"):
        con = duckdb.connect()
        con.execute("INSTALL delta; LOAD delta;")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            CREATE OR REPLACE SECRET minio_secret (
                TYPE s3,
                KEY_ID '{MINIO_KEY}',
                SECRET '{MINIO_SECRET}',
                ENDPOINT '{MINIO_ENDPOINT}',
                USE_SSL false,
                URL_STYLE 'path'
            );
        """)
        _local.con = con
    return _local.con
```

### 7.3 Ví dụ route

```python
# routes/keywords.py
from fastapi import APIRouter
from app.db.duckdb_session import get_con

router = APIRouter(prefix="/keywords", tags=["keywords"])

@router.get("/daily/{ingested_at}")
def keyword_daily(ingested_at: str):
    con = get_con()
    rows = con.execute("""
        SELECT keyword, marketplace, total_products, avg_price, sponsored_pct
        FROM delta_scan('s3://lakehouse/gold/amazon/mart_keyword_daily/')
        WHERE ingested_at = ?
        ORDER BY total_products DESC
    """, [ingested_at]).fetchall()
    return rows
```

---

## 8. Tích hợp Superset

### 8.1 Cài đặt driver

Trong môi trường Superset (Docker hoặc venv):

```bash
pip install duckdb-engine duckdb
```

### 8.2 SQLAlchemy connection string

Superset kết nối DuckDB qua một **file database** (`.duckdb`). Superset không thể dùng DuckDB in-memory vì nó cần persistent connection.

Cách tiếp cận: tạo một file DuckDB dùng làm **read-only query interface** — file này không chứa data, chỉ chứa **views** trỏ vào MinIO.

```
Connection string:
duckdb:////path/to/superset_views.duckdb
```

### 8.3 Script khởi tạo views

```python
# scripts/setup_superset_duckdb.py
import duckdb

DB_PATH = "./superset_views.duckdb"
MINIO_ENDPOINT = "localhost:9000"
MINIO_KEY = "tntkhai"
MINIO_SECRET = "14442002"

con = duckdb.connect(DB_PATH)
con.execute("INSTALL delta; LOAD delta;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    CREATE OR REPLACE SECRET minio_secret (
        TYPE s3,
        KEY_ID '{MINIO_KEY}',
        SECRET '{MINIO_SECRET}',
        ENDPOINT '{MINIO_ENDPOINT}',
        USE_SSL false,
        URL_STYLE 'path'
    );
""")

GOLD = "s3://lakehouse/gold/amazon"

# Gold views — Superset sẽ thấy các "tables" này
con.execute(f"""
    CREATE OR REPLACE VIEW dim_product AS
    SELECT * FROM delta_scan('{GOLD}/dim_product/')
    WHERE is_current = true;
""")

con.execute(f"""
    CREATE OR REPLACE VIEW fact_search_ranking AS
    SELECT * FROM delta_scan('{GOLD}/fact_search_ranking/');
""")

con.execute(f"""
    CREATE OR REPLACE VIEW fact_price_snapshot AS
    SELECT * FROM delta_scan('{GOLD}/fact_price_snapshot/');
""")

con.execute(f"""
    CREATE OR REPLACE VIEW fact_product_performance AS
    SELECT * FROM delta_scan('{GOLD}/fact_product_performance/');
""")

con.execute(f"""
    CREATE OR REPLACE VIEW mart_keyword_daily AS
    SELECT * FROM delta_scan('{GOLD}/mart_keyword_daily/');
""")

con.execute(f"""
    CREATE OR REPLACE VIEW mart_brand_competitive AS
    SELECT * FROM delta_scan('{GOLD}/mart_brand_competitive/');
""")

con.close()
print(f"Views created in {DB_PATH}")
```

Chạy script này một lần sau khi khởi động Docker. Sau khi Gold data được materialise thêm, các view tự động thấy dữ liệu mới (vì chúng scan Delta table, không cache).

### 8.4 Thêm database trong Superset UI

```
Database Driver : duckdb+duckdb_engine
SQLAlchemy URI  : duckdb:////absolute/path/to/superset_views.duckdb?read_only=true
```

---

## 9. Thay đổi docker-compose.yml

Chỉ cần thêm **một service** — Superset. DuckDB không cần container riêng.

```yaml
services:
  # ... các service hiện tại (minio, spark-master, spark-worker) ...

  superset:
    image: apache/superset:latest
    container_name: superset
    ports:
      - "8088:8088"
    volumes:
      - ./superset_views.duckdb:/app/superset_views.duckdb:ro
    environment:
      SUPERSET_SECRET_KEY: "your-superset-secret"
    networks:
      - lakehouse-tntkhai
    depends_on:
      - minio
    restart: unless-stopped
```

`FastAPI` service sẽ thêm sau khi bạn xây dựng API layer. DuckDB chạy trực tiếp trong process của FastAPI — không cần entry trong docker-compose.

---

## 10. Lộ trình migration lên Trino (khi cần)

Khi dự án tăng trưởng đến điểm này, thì migrate:

| Tín hiệu | Ngưỡng cụ thể |
|---|---|
| Query chậm | Scan Gold table > 10 giây ở DuckDB |
| Data volume lớn | Gold layer > 50 GB |
| Concurrent users | > 10 user query Superset đồng thời |
| Multi-cluster | Cần đọc từ nhiều data source khác nhau |

Migration path khi đó: thêm HMS + Trino vào docker-compose, đăng ký Delta tables vào Hive Metastore, cập nhật Superset connection string từ `duckdb://` sang `trino://`. Dagster pipeline và Gold layer **không cần thay đổi gì** vì chúng chỉ ghi vào MinIO.

---

## 11. Checklist triển khai

```
[ ] pip install duckdb duckdb-engine      (thêm vào pyproject.toml)
[ ] Chạy scripts/setup_superset_duckdb.py sau mỗi lần thêm Gold table mới
[ ] Mount superset_views.duckdb vào Superset container (read-only)
[ ] Thêm superset service vào docker-compose.yml
[ ] Viết db/duckdb_session.py trong FastAPI project
[ ] Test: query mart_keyword_daily từ Python shell trước khi wiring Superset
```
