import dagster as dg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ...resources.spark_resource import SparkResource

FACT_PERFORMANCE_PATH = "s3a://lakehouse/gold/amazon/fact_product_performance"
DIM_PRODUCT_PATH = "s3a://lakehouse/gold/amazon/dim_product"
GOLD_PATH = "s3a://lakehouse/gold/amazon/mart_brand_competitive"

daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-03-04")


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------


def _transform(fact_df, dim_df):
    """Aggregates product performance to brand × category × day KPIs.

    Grain: one row per (brand, category_leaf, marketplace, ingested_at).

    Join strategy: use product_sk (present in both fact and dim).
    Rows with null brand are excluded — they represent products with incomplete
    dimension data and would pollute brand-level benchmarks.
    """
    dim_current = (
        dim_df
        .filter(F.col("is_current") == True)
        .select("product_sk", "brand", "category_leaf")
    )

    return (
        fact_df
        .join(dim_current, on="product_sk", how="left")
        .filter(F.col("brand").isNotNull())
        .groupBy("brand", "category_leaf", "marketplace", "ingested_at")
        .agg(
            F.countDistinct("asin").alias("asin_count"),
            F.round(F.avg("bsr_primary_rank"), 0).cast("long").alias("avg_bsr_primary"),
            F.min("bsr_primary_rank").alias("min_bsr_primary"),
            F.round(F.avg("rating_weighted_avg"), 2).alias("avg_rating_weighted"),
            F.round(
                F.avg(F.col("in_stock").cast("int")) * 100, 2
            ).alias("in_stock_rate"),
            F.sum(F.col("is_fba").cast("int")).alias("fba_count"),
            F.round(F.avg("variation_count"), 1).alias("avg_variation_count"),
            F.round(F.avg("color_count"), 1).alias("avg_color_count"),
            F.round(F.avg("size_count"), 1).alias("avg_size_count"),
        )
        .withColumn(
            "fba_pct",
            F.when(
                F.col("asin_count") > 0,
                F.round(F.col("fba_count") / F.col("asin_count") * 100, 2),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("gold_processed_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    partitions_def=daily_partitions,
    compute_kind="pyspark",
    group_name="gold",
    key_prefix=["gold", "amazon"],
    name="mart_brand_competitive",
    deps=[
        dg.AssetKey(["gold", "amazon", "fact_product_performance"]),
        dg.AssetKey(["gold", "amazon", "dim_product"]),
    ],
    metadata={
        "description": (
            "One row per (brand, category_leaf, marketplace, ingested_at). "
            "Brand-level competitive benchmarks: ASIN count, average/best BSR, "
            "weighted rating, in-stock rate, FBA penetration, and catalogue breadth. "
            "Rows with null brand are excluded. "
            "Partitioned by ingested_at. Written with replaceWhere (idempotent)."
        )
    },
)
def gold_amazon_mart_brand_competitive(
    context: dg.AssetExecutionContext,
    spark: SparkResource,
) -> dg.Output[None]:
    """Gold fact_product_performance + dim_product → Gold mart_brand_competitive."""
    partition_date: str = context.partition_key
    session: SparkSession = spark.get_session()

    fact_df = (
        session.read.format("delta")
        .load(FACT_PERFORMANCE_PATH)
        .filter(F.col("ingested_at") == partition_date)
    )

    row_count_fact = fact_df.count()
    if row_count_fact == 0:
        context.log.warning(
            f"fact_product_performance partition {partition_date} is empty — skipping."
        )
        return dg.Output(
            value=None,
            metadata={"partition_date": partition_date, "fact_row_count": 0},
        )

    dim_df = session.read.format("delta").load(DIM_PRODUCT_PATH)

    mart = _transform(fact_df, dim_df)
    row_count_mart = mart.count()

    context.log.info(
        f"Writing mart_brand_competitive: {row_count_mart} rows → {GOLD_PATH}"
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
