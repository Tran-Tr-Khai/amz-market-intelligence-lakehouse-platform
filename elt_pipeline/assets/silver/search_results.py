import re

import dagster as dg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ...resources.spark_resource import SparkResource

BRONZE_PATH = "s3a://lakehouse/bronze/amazon/search_results"
SILVER_PATH = "s3a://lakehouse/silver/amazon/search_results"

daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-03-04")

def _parse_price(col_name: str) -> tuple[F.Column, F.Column, F.Column]:
    """Parses raw price strings like 'VND 340,600' or '$12.99'.

    Returns three columns:
        price_raw       - unchanged original string (audit trail)
        price_currency  - currency code / symbol (e.g. 'VND', '$')
        price_amount    - numeric value as DoubleType
    """
    raw = F.col(col_name)
    currency = F.regexp_extract(raw, r"^([^\d]+)", 1)
    amount_str = F.regexp_replace(
        F.regexp_extract(raw, r"([\d,]+(?:\.\d+)?)", 1),
        ",",
        "",
    )
    amount = F.when(amount_str != "", amount_str.cast("double"))
    return raw.alias(f"{col_name}_raw"), F.trim(currency).alias(f"{col_name}_currency"), amount.alias(f"{col_name}_amount")

def _parse_rating_value(col_name: str = "rating") -> F.Column:
    """Extracts numeric rating from '4.5 out of 5 stars' → 4.5 (DoubleType)."""
    extracted = F.regexp_extract(F.col(col_name), r"^([\d.]+)", 1)
    return F.when(extracted != "", extracted.cast("double")).alias("rating")

def _parse_ratings_count(col_name: str = "ratings_count") -> F.Column:
    """Parses review count strings like '(130.9K)' or '(3.4K)' → LongType.

    Handles K (thousands) and M (millions) suffixes – no suffix means the number
    is already an integer (e.g. '(22)').
    """
    stripped = F.regexp_extract(F.col(col_name), r"\(([\d.]+)([KkMm]?)\)", 1)
    suffix = F.upper(F.regexp_extract(F.col(col_name), r"\(([\d.]+)([KkMm]?)\)", 2))
    multiplier = (
        F.when(suffix == "K", F.lit(1_000))
        .when(suffix == "M", F.lit(1_000_000))
        .otherwise(F.lit(1))
    )
    return F.when(stripped != "", (stripped.cast("double") * multiplier).cast("long")).alias("ratings_count")

def _parse_monthly_sales(col_name: str = "monthly_sales") -> F.Column:
    """Extracts the lower-bound integer from strings like '1K+ bought in past month'.

    Returns null when the column is null (product has no monthly sales badge).
    Examples:
        '1K+ bought in past month' → 1000
        '300+ bought in past month' → 300
        '2K+ bought in past month' → 2000
    """
    num_part = F.regexp_extract(F.col(col_name), r"([\d.]+)([KkMm]?)", 1)
    suffix = F.upper(F.regexp_extract(F.col(col_name), r"([\d.]+)([KkMm]?)", 2))
    multiplier = (
        F.when(suffix == "K", F.lit(1_000))
        .when(suffix == "M", F.lit(1_000_000))
        .otherwise(F.lit(1))
    )
    result = F.when(num_part != "", (num_part.cast("double") * multiplier).cast("long"))
    return F.when(F.col(col_name).isNotNull(), result).alias("monthly_sales")

def _parse_coupon(col_name: str = "coupon") -> tuple[F.Column, F.Column]:
    """Extracts coupon metadata from raw text like 'Save 15%with coupon (some sizes/colors)'.

    Returns:
        has_coupon           - BooleanType (true even when discount % is absent)
        coupon_discount_pct  - DoubleType, nullable (null if no % in text)
    """
    has_coupon = F.col(col_name).isNotNull().alias("has_coupon")
    pct_str = (F.regexp_extract(F.col(col_name), r"Save\s*([\d.]+)\s*%", 1))
    pct = F.when(pct_str != "", pct_str.cast("double"))
    coupon_discount_pct = F.when(F.col(col_name).isNotNull(), pct).alias("coupon")
    return has_coupon, coupon_discount_pct

# ---------------------------------------------------------------------------
# Core transformation
# ---------------------------------------------------------------------------

def _transform(bronze_df: DataFrame) -> DataFrame:
    """Full silver transformation pipeline for amazon_search_results.

    Steps
    -----
    1. Parse / clean each field
    2. Derive discount_pct from price / original_price
    3. Deduplicate: keep the most recent snapshot per (asin, keyword) within the day
    4. Drop bronze-only audit columns
    5. Add silver_processed_at timestamp
    """

    # Parse individual fields 
    price_raw, price_currency, price_amount = _parse_price("price")
    orig_raw, orig_currency, orig_amount = _parse_price("original_price")
    has_coupon, coupon_discount_pct = _parse_coupon()

    df = bronze_df.select(
        "asin",
        F.trim(F.col("title")).alias("title"),
        # Price
        price_raw,
        price_currency,
        price_amount,
        orig_amount.alias("original_price_amount"),
        # Discount
        F.when(
            (orig_amount > 0) & orig_amount.isNotNull() & price_amount.isNotNull(),
            F.round((orig_amount - price_amount) / orig_amount * 100, 2),
        ).alias("discount_pct"),
        # Ratings
        _parse_rating_value(),
        _parse_ratings_count(),
        # Monthly sales
        _parse_monthly_sales(),
        # Badges
        F.col("is_sponsored"),
        F.col("is_prime"),
        F.col("is_best_seller"),
        F.col("is_amazons_choice"),
        # Coupon
        has_coupon,
        coupon_discount_pct,
        # Delivery
        F.col("delivery"),
        # URLs
        F.col("url"),
        F.col("image_url"),
        # Provenance
        F.col("keyword"),
        F.col("marketplace"),
        # Timestamps
        F.to_timestamp(F.col("source_extracted_at")).alias("source_extracted_at"),
        F.to_date(F.col("ingested_at")).alias("ingested_at"),
        # NOTE: source_file intentionally dropped — it is an implementation detail
        # of the bronze ingest stage and has no analytical value in silver.
    )

    # Dedup: one row per (asin, keyword) per day, latest snapshot wins 
    # This handles the case where the same ASIN appears in multiple source files
    # (e.g., two keyword-search pages scraped at different times on the same day).
    dedup_window = Window.partitionBy("asin", "keyword", "ingested_at").orderBy(
        F.col("source_extracted_at").desc()
    )
    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Add processing timestamp 
    df = df.withColumn("silver_processed_at", F.current_timestamp())

    return df

# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------

@dg.asset(
    partitions_def=daily_partitions, 
    compute_kind="pyspark",
    group_name="silver",
    key_prefix=["silver", "amazon"],
    name="search_results",
    deps=[dg.AssetKey(["bronze", "amazon", "search_results"])],
    metadata={
        "description": (
            "Cleaned and parsed view of bronze amazon_search_results. "
            "Prices, ratings, and counts are cast to native numeric types. "
            "Deduplicated to one row per (asin, keyword) per ingestion day. "
            "Partitioned by ingested_at for efficient downstream reads."
        )
    },
)
def silver_amazon_search_results(
    context: dg.AssetExecutionContext,
    spark: SparkResource,
) -> dg.Output[None]:
    """Bronze → Silver transformation for amazon_search_results.

    Reads the bronze partition for the current Dagster partition key, applies
    the full transformation pipeline, and writes the result back to MinIO as a
    Delta table partitioned by ``ingested_at``.

    The operation is idempotent: re-materialising the same partition replaces
    only that day's data via Delta's ``replaceWhere`` predicate.
    """
    partition_date: str = context.partition_key
    session: SparkSession = spark.get_session()

    context.log.info(f"Reading bronze partition ingested_at={partition_date}")

    bronze_df = (
        session.read.format("delta")
        .load(BRONZE_PATH)
        .filter(F.col("ingested_at") == partition_date)
    )

    row_count_bronze = bronze_df.count()
    if row_count_bronze == 0:
        context.log.warning(
            f"Bronze partition ingested_at={partition_date} is empty — skipping silver write."
        )
        return dg.Output(
            value=None,
            metadata={"partition_date": partition_date, "bronze_row_count": 0, "silver_row_count": 0},
        )

    context.log.info(f"Bronze rows read: {row_count_bronze}")

    silver_df = _transform(bronze_df)

    row_count_silver = silver_df.count()
    context.log.info(f"Silver rows after transform+dedup: {row_count_silver}")

    # Partition-level atomic overwrite — idempotent, re-run safe.
    # replaceWhere ensures only the current day's partition is replaced; other
    # partitions are left entirely untouched (no full-table scan or rewrite).
    context.log.info(f"Writing silver partition ingested_at={partition_date} → {SILVER_PATH}")

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingested_at = '{partition_date}'")
        .partitionBy("ingested_at")
        .save(SILVER_PATH)
    )

    context.log.info("Silver write complete.")

    return dg.Output(
        value=None,
        metadata={
            "partition_date": partition_date,
            "bronze_row_count": dg.MetadataValue.int(row_count_bronze),
            "silver_row_count": dg.MetadataValue.int(row_count_silver),
            "silver_path": dg.MetadataValue.text(SILVER_PATH),
            "write_mode": dg.MetadataValue.text("overwrite (replaceWhere)"),
        },
    )
