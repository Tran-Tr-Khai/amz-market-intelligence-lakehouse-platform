import dagster as dg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ...resources.spark_resource import SparkResource

BRONZE_PATH = "s3a://lakehouse/bronze/amazon/product_details"
SILVER_PATH = "s3a://lakehouse/silver/amazon/product_details"

daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-03-04")


# ---------------------------------------------------------------------------
# Field parsers
# ---------------------------------------------------------------------------


def _clean_brand(col_name: str = "brand") -> F.Column:
    """Strips 'Brand: ' prefix and trims whitespace."""
    return F.trim(F.regexp_replace(F.col(col_name), r"^Brand:\s*", "")).alias("brand")


def _parse_bsr() -> F.Column:
    """Parses every entry in the best_seller_rank array into a typed struct.

    Input example:
        ["#364 in Clothing, Shoes & Jewelry", "#1 inWomen's Fashion Sweatshirts"]

    Returns:
        best_seller_ranks – ArrayType(StructType([rank: IntegerType, category: StringType]))

    All entries are processed dynamically, so products with more than two BSR
    categories are handled correctly without any code changes.
    """
    rank_re = r"#?([\d,]+)\s+in\s*"
    cat_re = r"#?[\d,]+\s+in\s*(.+)$"

    return F.transform(
        F.col("best_seller_rank"),
        lambda item: F.struct(
            F.when(
                item.isNotNull(),
                F.regexp_replace(F.regexp_extract(item, rank_re, 1), ",", "").cast("int"),
            ).alias("rank"),
            F.when(
                item.isNotNull(),
                F.trim(F.regexp_extract(item, cat_re, 1)),
            ).alias("category"),
        ),
    ).alias("best_seller_ranks")


def _parse_dimensions() -> tuple[F.Column, F.Column, F.Column, F.Column]:
    """Parses dimension strings into length, width, height (Double) + unit.

    Supports both 3D ('12.68 x 10.91 x 3.31 inches') and 2D ('12 x 10 inches')
    formats. Height is null for 2D items such as posters or rugs.
    """
    pattern = r"^([\d.]+)\s*x\s*([\d.]+)(?:\s*x\s*([\d.]+))?\s*(\w+)"
    col = F.col("dimensions")

    def _part(group: int, alias: str, as_double: bool = True) -> F.Column:
        extracted = F.when(col.isNotNull(), F.regexp_extract(col, pattern, group))
        return (extracted.cast("double") if as_double else F.trim(extracted)).alias(alias)

    return (
        _part(1, "dimension_length"),
        _part(2, "dimension_width"),
        _part(3, "dimension_height"),
        _part(4, "dimension_unit", as_double=False),
    )


def _parse_weight() -> tuple[F.Column, F.Column]:
    """Parses '14.25 ounces' → weight_value (Double) + weight_unit (String)."""
    pattern = r"^([\d.]+)\s+(.+)$"
    col = F.col("weight")
    value = F.when(col.isNotNull(), F.regexp_extract(col, pattern, 1).cast("double")).alias(
        "weight_value"
    )
    unit = F.when(col.isNotNull(), F.trim(F.regexp_extract(col, pattern, 2))).alias(
        "weight_unit"
    )
    return value, unit


def _parse_rating_overview() -> list[F.Column]:
    """Expands the rating_overview struct into per-star columns and computes
    a weighted average rating.

    bronze ``rating_overview`` is a struct with integer fields "1"–"5" that
    represent the percentage share of each star rating (they sum to ~100).

    weighted_avg_rating formula:
        Σ(star_i × pct_i) / Σ(pct_i)
    """
    star = lambda n: F.coalesce(
        F.col("rating_overview").getField(str(n)).cast("double"), F.lit(0.0)
    )
    p5, p4, p3, p2, p1 = star(5), star(4), star(3), star(2), star(1)

    total = p5 + p4 + p3 + p2 + p1
    weighted_sum = 5 * p5 + 4 * p4 + 3 * p3 + 2 * p2 + 1 * p1

    return [
        p5.alias("rating_pct_5"),
        p4.alias("rating_pct_4"),
        p3.alias("rating_pct_3"),
        p2.alias("rating_pct_2"),
        p1.alias("rating_pct_1"),
        F.when(total > 0, F.round(weighted_sum / total, 2)).alias("rating_weighted_avg"),
    ]


# ---------------------------------------------------------------------------
# Core transformation
# ---------------------------------------------------------------------------


def _transform(bronze_df: DataFrame) -> DataFrame:
    """Full silver transformation pipeline for amazon_product_details.

    Steps
    -----
    1. Parse / clean every raw field into native types.
    2. Derive computed columns (BSR, weighted rating, dimension components).
    3. Deduplicate: keep one row per (asin, ingested_at), latest snapshot wins.
    4. Attach silver_processed_at audit timestamp.
    """
    best_seller_ranks = _parse_bsr()
    dim_length, dim_width, dim_height, dim_unit = _parse_dimensions()
    weight_value, weight_unit = _parse_weight()
    rating_cols = _parse_rating_overview()

    df = bronze_df.select(
        # --- Identity -----------------------------------------------------------
        F.col("asin"),
        F.col("parent_asin"),
        F.trim(F.col("title")).alias("title"),
        _clean_brand(),
        # --- Category -----------------------------------------------------------
        F.col("category").alias("category_path"),
        F.size(F.col("categories")).alias("category_depth"),
        F.element_at(F.col("categories"), -1).getField("name").alias("category_leaf"),
        # --- Best Seller Rank ---------------------------------------------------
        best_seller_ranks,
        # --- Physical attributes ------------------------------------------------
        F.col("dimensions").alias("dimensions_raw"),
        dim_length,
        dim_width,
        dim_height,
        dim_unit,
        weight_value,
        weight_unit,
        F.coalesce(
            F.to_date(F.trim(F.col("date_first_available")), "MMMM d, yyyy"),
            F.to_date(F.trim(F.col("date_first_available")), "MMM d, yyyy"),
            F.to_date(F.trim(F.col("date_first_available")), "yyyy-MM-dd"),
        ).alias("date_first_available"),
        # --- Variations ---------------------------------------------------------
        F.col("variation_count"),
        F.size(F.col("available_dimensions").getField("size_name")).alias("size_count"),
        F.size(F.col("available_dimensions").getField("color_name")).alias("color_count"),
        # --- Media --------------------------------------------------------------
        F.col("images").getItem(0).alias("main_image_url"),
        F.col("images").alias("all_image_urls"),
        F.size(F.col("images")).alias("image_count"),
        # --- Rating breakdown ---------------------------------------------------
        *rating_cols,
        # --- Availability & fulfilment ------------------------------------------
        (F.lower(F.col("stock_status")) == "in stock").alias("in_stock"),
        F.col("buybox_seller"),
        F.col("is_fba"),
        # --- Provenance ---------------------------------------------------------
        F.col("marketplace"),
        F.to_timestamp(F.col("source_extracted_at")).alias("source_extracted_at"),
        F.to_date(F.col("ingested_at")).alias("ingested_at"),
    )

    # Dedup: one row per (asin, ingested_at), latest scrape snapshot wins.
    # Guards against duplicate files ingested for the same product on the same day.
    dedup_window = Window.partitionBy("asin", "ingested_at").orderBy(
        F.col("source_extracted_at").desc()
    )
    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    return df.withColumn("silver_processed_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    partitions_def=daily_partitions,
    compute_kind="pyspark",
    group_name="silver",
    key_prefix=["silver", "amazon"],
    name="product_details",
    deps=[dg.AssetKey(["bronze", "amazon", "product_details"])],
    metadata={
        "description": (
            "Cleaned and typed view of bronze amazon_product_details. "
            "Brand prefix stripped; BSR parsed into rank + category; dimensions and weight "
            "split into numeric columns; rating_overview expanded with weighted average. "
            "Deduplicated to one row per (asin) per ingestion day."
        )
    },
)
def silver_amazon_product_details(
    context: dg.AssetExecutionContext,
    spark: SparkResource,
) -> dg.Output[None]:
    """Bronze → Silver transformation for amazon_product_details.

    Reads the bronze partition for the current Dagster partition key, applies
    the full transformation pipeline, and writes the result to MinIO as a Delta
    table partitioned by ``ingested_at``.

    Idempotent: re-materialising the same partition replaces only that day's data
    via Delta's ``replaceWhere`` predicate.
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
            metadata={
                "partition_date": partition_date,
                "bronze_row_count": 0,
                "silver_row_count": 0,
            },
        )

    context.log.info(f"Bronze rows read: {row_count_bronze}")

    silver_df = _transform(bronze_df)

    row_count_silver = silver_df.count()
    context.log.info(f"Silver rows after transform+dedup: {row_count_silver}")

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
