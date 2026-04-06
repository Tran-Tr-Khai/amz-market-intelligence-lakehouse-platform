import dagster as dg
import polars as pl
from typing import Union, Literal

from ..resources.minio_resource import MinioResource


class MinioIOManager(dg.ConfigurableIOManager):
    minio: MinioResource
    bucket_name: str = "lakehouse"
    # Default write mode for all assets; can be overridden per-asset via
    # output metadata: {"write_mode": "overwrite"}
    write_mode: Literal["append", "overwrite", "error", "ignore"] = "append"
    # Optional list of column names to physically partition the Delta table by.
    # Example: ["ingested_at"]  → enables efficient partition-level reads/overwrites.
    partition_by: list[str] = []

    def _get_s3_path(self, context: Union[dg.InputContext, dg.OutputContext]) -> str:
        """Returns the S3 URI for the Delta table *directory*.

        Delta tables are always directories (they contain _delta_log/, part files, etc.).
        The partition key is managed internally by Delta Lake — it must NOT appear in
        the table path; it becomes a column filter at read/write time.

        asset_key = ["bronze", "amazon", "search_results"]
        → s3://lakehouse/bronze/amazon/search_results/
        """
        path_parts = context.asset_key.path
        return f"s3://{self.bucket_name}/{'/'.join(path_parts)}"

    def _get_storage_options(self) -> dict:
        """Returns storage options in the format expected by the *deltalake* (delta-rs)
        package.

        Important: delta-rs uses uppercase, env-var-style keys (AWS_*), which differ
        from the lowercase snake_case keys accepted by plain Polars S3 connectors.
        AWS_S3_ALLOW_UNSAFE_RENAME is required for MinIO and all S3-compatible stores
        because they do not support the atomic renames that Delta protocol relies on.
        """
        protocol = "https" if self.minio.secure else "http"
        options: dict[str, str] = {
            "AWS_ENDPOINT_URL": f"{protocol}://{self.minio.endpoint}",
            "AWS_ACCESS_KEY_ID": self.minio.access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio.secret_key,
            "AWS_SESSION_TOKEN": "",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
        if not self.minio.secure:
            options["AWS_ALLOW_HTTP"] = "true"
        return options

    def handle_output(self, context: dg.OutputContext, obj: pl.DataFrame) -> None:
        if obj.is_empty():
            context.log.warning("Received empty DataFrame — skipping Delta write.")
            return

        s3_path = self._get_s3_path(context)
        storage_options = self._get_storage_options()

        # Per-asset override: add `metadata={"write_mode": "overwrite"}` to @dg.asset
        write_mode: str = (
            context.definition_metadata.get("write_mode", self.write_mode)
            if context.definition_metadata
            else self.write_mode
        )

        client = self.minio.get_client()
        if not client.bucket_exists(self.bucket_name):
            client.make_bucket(self.bucket_name)
            context.log.info(f"Created bucket: {self.bucket_name}")

        delta_write_options: dict = {}
        if self.partition_by:
            delta_write_options["partition_by"] = self.partition_by

        context.log.info(
            f"Writing DataFrame {obj.shape} to {s3_path} "
            f"(mode={write_mode}, partition_by={self.partition_by or 'none'})"
        )

        obj.write_delta(
            s3_path,
            mode=write_mode,
            storage_options=storage_options,
            delta_write_options=delta_write_options or None,
        )

        context.add_output_metadata({
            "path": s3_path,
            "write_mode": write_mode,
            "row_count": obj.shape[0],
            "column_count": obj.shape[1],
            "columns": obj.columns,
        })

    def load_input(self, context: dg.InputContext) -> pl.DataFrame:
        s3_path = self._get_s3_path(context)
        storage_options = self._get_storage_options()

        context.log.info(f"Reading Delta table from {s3_path}")
        
        # Sử dụng Lazy API để đọc
        lazy_df = pl.scan_delta(
            s3_path,
            storage_options=storage_options,
        )

        # Đẩy bộ lọc xuống thẳng thư mục MinIO nếu có partition key
        if context.has_partition_key and self.partition_by:
            partition_key = context.asset_partition_key
            partition_col = self.partition_by[0]
            
            context.log.info(f"Pushdown filter: {partition_col} == {partition_key}")
            lazy_df = lazy_df.filter(pl.col(partition_col) == partition_key)

        return lazy_df.collect()