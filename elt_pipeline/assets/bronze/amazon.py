from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl

DATA_ROOT = Path(__file__).resolve().parent.parent.parent / "data"


START_DATE = "2026-03-04"

daily_partitions = dg.DailyPartitionsDefinition(start_date=START_DATE)

def _extract_metadata_value(df: pl.DataFrame, field_name: str) -> Any:
    return df.select(pl.col("metadata").struct.field(field_name)).item()

def _normalize_null_columns(context: dg.AssetExecutionContext, df: pl.DataFrame) -> pl.DataFrame:
    null_columns = [
        column_name
        for column_name, dtype in df.schema.items()
        if dtype == pl.Null
    ]

    if not null_columns:
        return df

    context.log.warning(f"Casting null-only columns to String: {null_columns}")
    return df.with_columns(pl.col(column_name).cast(pl.String) for column_name in null_columns)


def _load_bronze_partition(
    context: dg.AssetExecutionContext,
    *,
    source_dir: str,
    metadata_fields: dict[str, str],
) -> pl.DataFrame:
    partition_date = context.partition_key
    local_dir = DATA_ROOT / source_dir / partition_date

    if not local_dir.exists():
        context.log.warning(f"Source folder does not exist: {local_dir}")
        return pl.DataFrame()

    json_files = sorted(local_dir.glob("*.json"))
    if not json_files:
        context.log.warning(f"No JSON files found in {local_dir}")
        return pl.DataFrame()

    frames: list[pl.DataFrame] = []

    for file_path in json_files:
        try:
            raw_df = pl.read_json(file_path)
            metadata_columns = [
                pl.lit(_extract_metadata_value(raw_df, metadata_field)).alias(column_name)
                for column_name, metadata_field in metadata_fields.items()
            ]

            products_df = (
                raw_df
                .select(pl.col("products").list.explode())
                .unnest("products")
                .with_columns(
                    [
                        *metadata_columns,
                        pl.lit(partition_date).alias("ingested_at"),
                        pl.lit(file_path.name).alias("source_file"),
                    ]
                )
            )

            frames.append(products_df)
        except Exception as exc:
            context.log.error(f"Failed to process {file_path}: {exc}")

    if not frames:
        context.log.warning(f"No valid records were loaded from {local_dir}")
        return pl.DataFrame()

    final_df = pl.concat(frames, how="diagonal_relaxed")
    final_df = _normalize_null_columns(context, final_df)

    context.log.info(f"Ingested {final_df.height} records from {len(frames)} file(s)")
    return final_df


@dg.asset(
    partitions_def=daily_partitions,
    io_manager_key="minio_io",
    compute_kind="polars",
    group_name="bronze",
    key_prefix=["bronze", "amazon"],
    name="search_results",
)
def amazon_search_results(context: dg.AssetExecutionContext) -> pl.DataFrame:
    return _load_bronze_partition(
        context,
        source_dir="search",
        metadata_fields={
            "keyword": "keyword",
            "marketplace": "marketplace",
            "source_extracted_at": "extracted_at",
        },
    )


@dg.asset(
    partitions_def=daily_partitions,
    io_manager_key="minio_io",
    compute_kind="polars",
    group_name="bronze",
    key_prefix=["bronze", "amazon"],
    name="product_details",
)
def amazon_product_details(context: dg.AssetExecutionContext) -> pl.DataFrame:
    return _load_bronze_partition(
        context,
        source_dir="details",
        metadata_fields={
            "marketplace": "marketplace",
            "source_extracted_at": "extracted_at",
        },
    )


