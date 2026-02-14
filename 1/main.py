from contextlib import asynccontextmanager
from typing import Annotated, AsyncGenerator

import asyncpg
import uvicorn
from fastapi import APIRouter, Depends, FastAPI, Request
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    db_host: str
    db_port: int = 5432
    db_user: str
    db_password: str = Field(alias="DB_PWD")
    db_name: str
    db_pool_min: int = 10
    db_pool_max: int = 20

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()  # type: ignore

    app.state.pool = await asyncpg.create_pool(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
    )
    yield
    await app.state.pool.close()


async def get_pg_connection(
    request: Request,
) -> AsyncGenerator[asyncpg.Connection, None]:
    """Получаем подключение из пула через app.state"""
    async with request.app.state.pool.acquire() as conn:
        yield conn


async def get_db_version(
    conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)],
):
    return await conn.fetchval("SELECT version()")


def register_routes(app: FastAPI):
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version)
    app.include_router(router)


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet", lifespan=lifespan)
    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
