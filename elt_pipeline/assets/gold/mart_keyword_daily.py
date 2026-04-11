import dagster as dg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ...resources.spark_resource import SparkResource

FACT_SEARCH_RANKING_PATH = "s3a://lakehouse/gold/amazon/fact_search_ranking"
GOLD_PATH = "s3a://lakehouse/gold/amazon/mart_keyword_daily"

daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-03-04")


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------


def _transform(fact_df):
    """Aggregates fact_search_ranking to keyword-level daily KPIs.

    Grain: one row per (keyword, marketplace, ingested_at).
    All metrics are pre-computed so Metabase/BI queries scan this small mart
    instead of the full fact table.
    """
    mart = (
        fact_df
        .groupBy("keyword", "marketplace", "ingested_at")
        .agg(
            F.countDistinct("asin").alias("total_products"),
            F.sum(F.col("is_sponsored").cast("int")).alias("sponsored_count"),
            F.sum(F.col("is_prime").cast("int")).alias("prime_count"),
            F.sum(F.col("is_best_seller").cast("int")).alias("best_seller_count"),
            F.sum(F.col("is_amazons_choice").cast("int")).alias("amazons_choice_count"),
            F.sum(F.col("has_coupon").cast("int")).alias("coupon_count"),
            F.round(F.avg("price_amount"), 2).alias("avg_price"),
            F.round(F.percentile_approx("price_amount", 0.5), 2).alias("median_price"),
            F.round(F.min("price_amount"), 2).alias("min_price"),
            F.round(F.max("price_amount"), 2).alias("max_price"),
            F.round(F.avg("rating_value"), 2).alias("avg_rating_value"),
            F.round(F.avg("ratings_count"), 0).cast("long").alias("avg_ratings_count"),
            F.sum("monthly_sales_min").alias("total_monthly_sales_min"),
        )
        .withColumn(
            "sponsored_pct",
            F.when(
                F.col("total_products") > 0,
                F.round(F.col("sponsored_count") / F.col("total_products") * 100, 2),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("gold_processed_at", F.current_timestamp())
    )
    return mart


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    partitions_def=daily_partitions,
    compute_kind="pyspark",
    group_name="gold",
    key_prefix=["gold", "amazon"],
    name="mart_keyword_daily",
    deps=[dg.AssetKey(["gold", "amazon", "fact_search_ranking"])],
    metadata={
        "description": (
            "One row per (keyword, marketplace, ingested_at). "
            "Pre-aggregated keyword KPIs: product count, badge distribution, "
            "price statistics, average rating, and total monthly sales floor. "
            "Designed for BI dashboards and keyword performance monitoring. "
            "Partitioned by ingested_at. Written with replaceWhere (idempotent)."
        )
    },
)
def gold_amazon_mart_keyword_daily(
    context: dg.AssetExecutionContext,
    spark: SparkResource,
) -> dg.Output[None]:
    """Gold fact_search_ranking → Gold mart_keyword_daily."""
    partition_date: str = context.partition_key
    session: SparkSession = spark.get_session()

    fact_df = (
        session.read.format("delta")
        .load(FACT_SEARCH_RANKING_PATH)
        .filter(F.col("ingested_at") == partition_date)
    )

    row_count_fact = fact_df.count()
    if row_count_fact == 0:
        context.log.warning(
            f"fact_search_ranking partition {partition_date} is empty — skipping."
        )
        return dg.Output(
            value=None,
            metadata={"partition_date": partition_date, "fact_row_count": 0},
        )

    mart = _transform(fact_df)
    row_count_mart = mart.count()

    context.log.info(
        f"Writing mart_keyword_daily: {row_count_mart} rows → {GOLD_PATH}"
    )

    (
        mart.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingested_at = '{partition_date}'")
        .partitionBy("ingested_at")
        .save(GOLD_PATH)
    )

    return dg.Output(
        value=None,
        metadata={
            "partition_date": partition_date,
            "fact_row_count": dg.MetadataValue.int(row_count_fact),
            "mart_row_count": dg.MetadataValue.int(row_count_mart),
            "gold_path": dg.MetadataValue.text(GOLD_PATH),
            "write_mode": dg.MetadataValue.text("overwrite (replaceWhere)"),
        },
    )
