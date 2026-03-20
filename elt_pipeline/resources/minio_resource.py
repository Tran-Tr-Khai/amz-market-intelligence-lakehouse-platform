from minio import Minio
from dagster import ConfigurableResource


class MinioResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = False    

    def get_client(self) -> Minio: 
        return Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        ) 