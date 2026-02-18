import asyncio

from client import GithubClient
from config import Settings
from scraper import GithubReposScrapper
from storage import ClickhouseStorage


async def main() -> None:
    settings = Settings()  # type: ignore

    async with GithubClient(
        access_token=settings.access_token,
        max_concurrent_requests=settings.max_concurrent_requests,
        requests_per_second=settings.requests_per_second,
    ) as client:
        storage = ClickhouseStorage(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )
        scraper = GithubReposScrapper(
            client=client,
            storage=storage,
            batch_size=settings.batch_size,
        )

        try:
            repositories = await scraper.get_repositories(limit=100)
            print(f"✅ Обработано и сохранено {len(repositories)} репозиториев")
            print(f"📊 Данные сохранены в 3 таблицы:")
            print(f"   - repositories: информация о репозиториях")
            print(f"   - repositories_positions: позиции в топе")
            print(f"   - repositories_authors_commits: коммиты авторов")
        finally:
            storage.close()


if __name__ == "__main__":
    asyncio.run(main())
