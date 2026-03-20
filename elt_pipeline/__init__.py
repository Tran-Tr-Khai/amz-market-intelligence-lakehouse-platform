from dotenv import load_dotenv

import dagster as dg

from .assets import bronze
from .io_manager import MinioIOManager
from .resources import MinioResource


load_dotenv()


minio_resource = MinioResource(
    endpoint=dg.EnvVar("MINIO_ENDPOINT"),
    access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
    secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
    secure=False,
)


defs = dg.Definitions(
    assets=dg.load_assets_from_modules([bronze]),
    resources={
        "minio_io": MinioIOManager(
            minio=minio_resource,
            bucket_name="lakehouse",
            partition_by=["ingested_at"],
        ),
        "minio_client": minio_resource,
    },
)