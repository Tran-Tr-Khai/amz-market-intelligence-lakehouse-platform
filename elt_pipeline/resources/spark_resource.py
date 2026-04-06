import os
from pathlib import Path

from dagster import ConfigurableResource
from pyspark.sql import SparkSession

# JARs pre-downloaded by spark/download_jars.sh and volume-mounted into
# Spark containers. Same directory is referenced here for the local[*] driver.
_JARS_DIR = Path(__file__).resolve().parent.parent.parent / "spark" / "jars"


def _extra_jars() -> str:
    """Returns a comma-separated list of JAR paths found in the jars directory."""
    jars = sorted(_JARS_DIR.glob("*.jar"))
    return ",".join(str(j) for j in jars)


class SparkResource(ConfigurableResource):
    """Configurable Dagster resource that creates a Delta Lake–enabled SparkSession.

    Default ``spark_master="local[*]"`` keeps the driver in-process with Dagster
    (no scheduling overhead, easy local dev). Switch to ``spark://spark-master:7077``
    once Dagster itself is containerised and network-reachable from the cluster.

    The ``minio_endpoint`` must be reachable from wherever the Spark *driver* runs:
    - local[*]  → http://localhost:9000  (MinIO port-forwarded to host)
    - cluster   → http://minio:9000      (minIO internal Docker network name)
    """

    spark_master: str = "local[*]"
    app_name: str = "amz-lakehouse"
    minio_endpoint: str = "http://localhost:9000"
    access_key: str
    secret_key: str

    def get_session(self) -> SparkSession:
        """Returns a configured SparkSession (creates one if none is active)."""
        # Reuse an existing active session — avoids Spark re-initialisation cost
        # when the same Dagster process materialises multiple assets in a run.
        existing = SparkSession.getActiveSession()
        if existing is not None:
            return existing

        extra_jars = _extra_jars()

        builder = (
            SparkSession.builder.master(self.spark_master)
            .appName(self.app_name)
            # ------ Delta Lake ------------------------------------------------
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # ------ JARs (driver + executor classpaths) -----------------------
            .config("spark.jars", extra_jars)
            # ------ S3A filesystem (MinIO) ------------------------------------
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # Required for MinIO / S3-compatible stores (no atomic rename support)
            .config(
                "spark.hadoop.fs.s3a.committer.name",
                "directory",
            )
            .config(
                "spark.delta.logStore.class",
                "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
            )
            # ------ Performance tuning (dev defaults) -------------------------
            .config("spark.sql.shuffle.partitions", "8")
            .config(
                "spark.serializer",
                "org.apache.spark.serializer.KryoSerializer",
            )
            # Suppress noisy INFO logs from Spark internals
            .config("spark.ui.showConsoleProgress", "false")
        )

        session = builder.getOrCreate()
        session.sparkContext.setLogLevel("WARN")
        return session
