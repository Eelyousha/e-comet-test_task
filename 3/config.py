from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    access_token: str = Field(alias="GITHUB_ACCESS_TOKEN", default="")
    clickhouse_host: str = Field(default="localhost")
    clickhouse_port: int = Field(default=8123)
    clickhouse_user: str = Field(default="default")
    clickhouse_password: str = Field(default="")
    clickhouse_database: str = Field(default="test")
    max_concurrent_requests: int = Field(default=10)
    requests_per_second: int = Field(default=10)
    batch_size: int = Field(default=50)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
