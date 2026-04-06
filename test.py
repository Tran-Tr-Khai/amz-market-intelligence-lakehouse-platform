import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# 1. Các hàm Parser (Đã thảo luận)
# ---------------------------------------------------------------------------
def _parse_price(col_name: str) -> tuple[F.Column, F.Column, F.Column]:
    raw = F.col(col_name)
    currency = F.regexp_extract(raw, r"^([^\d]+)", 1)
    
    # Tách riêng phần chuỗi số lượng
    amount_str = F.regexp_replace(
        F.regexp_extract(raw, r"([\d,]+(?:\.\d+)?)", 1),
        ",",
        ""
    )
    
    # Chỉ cast khi chuỗi trích ra không bị rỗng
    amount = F.when(amount_str != "", amount_str.cast("double"))
    
    return raw.alias(f"{col_name}_raw"), F.trim(currency).alias(f"{col_name}_currency"), amount.alias(f"{col_name}_amount")

def _parse_rating_value(col_name: str = "rating") -> F.Column:
    extracted = F.regexp_extract(F.col(col_name), r"^([\d.]+)", 1)
    return F.when(extracted != "", extracted.cast("double")).alias("rating_value")

def _parse_ratings_count(col_name: str = "ratings_count") -> F.Column:
    stripped = F.regexp_extract(F.col(col_name), r"\(([\d.]+)([KkMm]?)\)", 1)
    suffix = F.upper(F.regexp_extract(F.col(col_name), r"\(([\d.]+)([KkMm]?)\)", 2))
    
    multiplier = (
        F.when(suffix == "K", F.lit(1_000))
        .when(suffix == "M", F.lit(1_000_000))
        .otherwise(F.lit(1))
    )
    
    # Bọc bằng F.when
    return F.when(stripped != "", (stripped.cast("double") * multiplier).cast("long")).alias("ratings_count")

def _parse_monthly_sales(col_name: str = "monthly_sales") -> F.Column:
    num_part = F.regexp_extract(F.col(col_name), r"([\d.]+)([KkMm]?)", 1)
    suffix = F.upper(F.regexp_extract(F.col(col_name), r"([\d.]+)([KkMm]?)", 2))
    
    multiplier = (
        F.when(suffix == "K", F.lit(1_000))
        .when(suffix == "M", F.lit(1_000_000))
        .otherwise(F.lit(1))
    )
    
    # Bọc bằng F.when
    result = F.when(num_part != "", (num_part.cast("double") * multiplier).cast("long"))
    return result.alias("monthly_sales_min")

def _parse_coupon(col_name: str = "coupon") -> tuple[F.Column, F.Column]:
    has_coupon = F.col(col_name).isNotNull().alias("has_coupon")
    
    # Tách việc extract phần trăm ra biến riêng
    pct_str = F.regexp_extract(F.col(col_name), r"Save\s*([\d.]+)\s*%", 1)
    
    # Chỉ ép sang double nếu tìm thấy phần trăm
    coupon_discount_pct = F.when(pct_str != "", pct_str.cast("double")).alias("coupon_discount_pct")
    
    return has_coupon, coupon_discount_pct
# ---------------------------------------------------------------------------
# 2. Hàm Transform Cốt Lõi
# ---------------------------------------------------------------------------
def _transform(bronze_df: DataFrame) -> DataFrame:
    # PHẲNG HÓA: Đưa data từ products và metadata ra ngoài
    flattened_df = bronze_df.select(
        F.explode(F.col("products")).alias("p"),
        F.col("metadata.extracted_at").alias("source_extracted_at"),
        F.col("ingested_at")
    ).select(
        "p.*", 
        "source_extracted_at", 
        "ingested_at"
    )
    
    # Bước 1: Gọi các parser
    price_raw, price_currency, price_amount = _parse_price("price")
    orig_raw, orig_currency, orig_amount = _parse_price("original_price")
    has_coupon, coupon_discount_pct = _parse_coupon()

    # Bước 2: Select và định dạng các cột
    df = flattened_df.select(
        "asin",
        F.trim(F.col("title")).alias("title"),
        price_raw, price_currency, price_amount,
        orig_amount.alias("original_price_amount"),
        F.when(
            (orig_amount > 0) & orig_amount.isNotNull() & price_amount.isNotNull(),
            F.round((orig_amount - price_amount) / orig_amount * 100, 2),
        ).alias("discount_pct"),
        _parse_rating_value(),
        _parse_ratings_count(),
        _parse_monthly_sales(),
        F.col("is_sponsored"), F.col("is_prime"), F.col("is_best_seller"), F.col("is_amazons_choice"),
        has_coupon, coupon_discount_pct,
        F.col("delivery"), F.col("url"), F.col("image_url"), F.col("keyword"), F.col("marketplace"),
        # Chuyển string thành timestamp chuẩn
        F.to_timestamp(F.col("source_extracted_at")).alias("source_extracted_at"),
        F.to_date(F.col("ingested_at")).alias("ingested_at"),
    )

    # Bước 3: Loại bỏ trùng lặp (Deduplication)
    # Sửa lỗi: Dùng đúng tên cột 'source_extracted_at'
    dedup_window = Window.partitionBy("asin", "keyword", "ingested_at").orderBy(
        F.col("source_extracted_at").desc()
    )
    
    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Bước 4: Thêm timestamp thời điểm xử lý của tầng Silver
    df = df.withColumn("silver_processed_at", F.current_timestamp())

    return df

# ---------------------------------------------------------------------------
# 3. Kịch bản chạy thử nghiệm (Main)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Khởi tạo Spark Session chạy tại máy (Local)
    spark = SparkSession.builder \
        .appName("Test_Amazon_Silver_Transform") \
        .master("local[*]") \
        .getOrCreate()

    # Tắt log rác của Spark để dễ nhìn kết quả hơn
    spark.sparkContext.setLogLevel("ERROR")

    # Đường dẫn đến file JSON thô của bạn
    # Dùng wildcard (*) để có thể đọc tất cả các file trong thư mục ngày
    data_path = "data/search/2026-03-04/search_sweatshirt_US_110356.json"
    
    print(f"Đang đọc dữ liệu từ: {data_path} ...")
    
    # Đọc file JSON. Tuỳ vào định dạng của Scraper mà bạn thêm option multiline nếu cần
    # option("multiline", "true") nếu file json là 1 array [{}, {}]
    raw_df = spark.read.option("multiline", "true").json(data_path)
    
    # MÔ PHỎNG DỮ LIỆU TẦNG BRONZE: 
    # File JSON scrape về có thể chưa có cột 'ingested_at' (thường được add ở script Bronze).
    # Nên ta sẽ giả lập nó ở đây để hàm _transform không bị lỗi.
    if "ingested_at" not in raw_df.columns:
        print("Tự động thêm cột 'ingested_at' để giả lập tầng Bronze...")
        raw_df = raw_df.withColumn("ingested_at", F.lit("2026-03-04"))
        
    # Chạy hàm transform
    print("Đang xử lý dữ liệu...")
    silver_df = _transform(raw_df)
    
    # In ra sơ đồ dữ liệu (Schema)
    print("\n--- SCHEMA SAU KHI TRANSFORM ---")
    silver_df.printSchema()
    
    # In ra 5 dòng đầu tiên để kiểm tra kết quả
    print("\n--- DỮ LIỆU MẪU (5 DÒNG) ---")
    silver_df.select(
        "asin", "price_amount", "discount_pct", "rating_value", 
        "ratings_count", "monthly_sales_min", "coupon_discount_pct"
    ).show(5, truncate=False)
    
    print("Test hoàn tất!")
    spark.stop()