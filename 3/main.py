import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Final

import clickhouse_connect
from aiohttp import ClientSession
from clickhouse_connect.driver.client import Client
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


GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"


@dataclass
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


class GithubReposScrapper:
    def __init__(
        self,
        access_token: str,
        clickhouse_host: str = "localhost",
        clickhouse_port: int = 8123,
        clickhouse_user: str = "default",
        clickhouse_password: str = "",
        clickhouse_database: str = "test",
        max_concurrent_requests: int = 10,
        requests_per_second: int = 10,
        batch_size: int = 50,
    ):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )
        # Семафор для ограничения максимального количества одновременных запросов
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)

        # Для ограничения RPS
        self._rps_limit = requests_per_second
        self._request_times: list[float] = []
        self._rps_lock = asyncio.Lock()

        # ClickHouse клиент
        self._ch_client: Client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_database,
        )

        # Размер батча для вставки
        self._batch_size = batch_size

    async def _rate_limit(self):
        """Ограничение количества запросов в секунду (RPS)"""
        async with self._rps_lock:
            now = asyncio.get_event_loop().time()

            # Удаляем запросы старше 1 секунды
            self._request_times = [t for t in self._request_times if now - t < 1.0]

            # Если достигли лимита, ждём
            if len(self._request_times) >= self._rps_limit:
                sleep_time = 1.0 - (now - self._request_times[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                # Очищаем старые записи после ожидания
                now = asyncio.get_event_loop().time()
                self._request_times = [t for t in self._request_times if now - t < 1.0]

            # Добавляем текущий запрос
            self._request_times.append(now)

    async def _make_request(
        self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None
    ) -> Any:
        # Применяем ограничения MCR и RPS
        async with self._semaphore:
            await self._rate_limit()
            async with self._session.request(
                method, f"{GITHUB_API_BASE_URL}/{endpoint}", params=params
            ) as response:
                return await response.json()

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories"""
        data = await self._make_request(
            endpoint="search/repositories",
            params={
                "q": "stars:>1",
                "sort": "stars",
                "order": "desc",
                "per_page": limit,
            },
        )
        return data["items"]

    async def _get_repository_commits(
        self, owner: str, repo: str
    ) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits"""
        # Получаем коммиты за последние 24 часа
        since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        data = await self._make_request(
            endpoint=f"repos/{owner}/{repo}/commits",
            params={"since": since, "per_page": 100},
        )
        return data if isinstance(data, list) else []

    async def _process_repository(
        self, repo_data: dict[str, Any], position: int
    ) -> Repository:
        """Обрабатывает один репозиторий: получает коммиты и подсчитывает авторов"""
        owner = repo_data["owner"]["login"]
        name = repo_data["name"]

        # Получаем коммиты за последний день
        commits = await self._get_repository_commits(owner, name)

        # Подсчитываем количество коммитов по авторам
        author_commits: dict[str, int] = {}
        for commit in commits:
            # Проверяем наличие автора
            if commit.get("author") and commit["author"].get("login"):
                author = commit["author"]["login"]
                author_commits[author] = author_commits.get(author, 0) + 1

        # Формируем список авторов с количеством коммитов
        authors_commits_num_today = [
            RepositoryAuthorCommitsNum(author=author, commits_num=count)
            for author, count in author_commits.items()
        ]

        return Repository(
            name=name,
            owner=owner,
            position=position,
            stars=repo_data["stargazers_count"],
            watchers=repo_data["watchers_count"],
            forks=repo_data["forks_count"],
            language=repo_data["language"] or "Unknown",
            authors_commits_num_today=authors_commits_num_today,
        )

    def _save_repositories_batch(self, repositories: list[Repository]) -> None:
        """Сохраняет батч репозиториев в таблицу repositories"""
        if not repositories:
            return

        current_time = datetime.now(timezone.utc)

        # Подготовка данных для таблицы repositories
        # Формат: name, owner, stars, watchers, forks, language, updated
        repo_data = [
            [
                repo.name,
                repo.owner,
                repo.stars,
                repo.watchers,
                repo.forks,
                repo.language,
                current_time,
            ]
            for repo in repositories
        ]

        # Вставка в таблицу repositories
        self._ch_client.insert(
            table="repositories",
            data=repo_data,
            column_names=[
                "name",
                "owner",
                "stars",
                "watchers",
                "forks",
                "language",
                "updated",
            ],
        )

    def _save_positions_batch(self, repositories: list[Repository]) -> None:
        """Сохраняет батч позиций репозиториев в таблицу repositories_positions"""
        if not repositories:
            return

        current_date = date.today()

        # Подготовка данных для таблицы repositories_positions
        # Формат: date, repo, position
        # repo формируется как "owner/name"
        positions_data = [
            [current_date, f"{repo.owner}/{repo.name}", repo.position]
            for repo in repositories
        ]

        # Вставка в таблицу repositories_positions
        self._ch_client.insert(
            table="repositories_positions",
            data=positions_data,
            column_names=["date", "repo", "position"],
        )

    def _save_commits_batch(self, repositories: list[Repository]) -> None:
        """Сохраняет батч данных о коммитах авторов в таблицу repositories_authors_commits"""
        if not repositories:
            return

        current_date = date.today()

        # Подготовка данных для таблицы repositories_authors_commits
        # Формат: date, repo, author, commits_num
        commits_data = []

        for repo in repositories:
            repo_full_name = f"{repo.owner}/{repo.name}"
            for author_commit in repo.authors_commits_num_today:
                commits_data.append(
                    [
                        current_date,
                        repo_full_name,
                        author_commit.author,
                        author_commit.commits_num,
                    ]
                )

        if not commits_data:
            return

        # Вставка в таблицу repositories_authors_commits
        self._ch_client.insert(
            table="repositories_authors_commits",
            data=commits_data,
            column_names=["date", "repo", "author", "commits_num"],
        )

    async def get_repositories(self, limit: int = 100) -> list[Repository]:
        """Получает топ репозиториев с информацией о коммитах за последний день и сохраняет в ClickHouse"""
        # Получаем топ репозиториев
        top_repos = await self._get_top_repositories(limit)

        all_repositories: list[Repository] = []
        batch: list[Repository] = []

        # Обрабатываем репозитории батчами
        for idx, repo_data in enumerate(top_repos):
            # Обрабатываем репозиторий
            repository = await self._process_repository(repo_data, position=idx + 1)
            all_repositories.append(repository)
            batch.append(repository)

            # Если батч заполнен, сохраняем его в ClickHouse
            if len(batch) >= self._batch_size:
                await asyncio.to_thread(self._save_repositories_batch, batch)
                await asyncio.to_thread(self._save_positions_batch, batch)
                await asyncio.to_thread(self._save_commits_batch, batch)
                batch = []

        # Сохраняем оставшиеся данные
        if batch:
            await asyncio.to_thread(self._save_repositories_batch, batch)
            await asyncio.to_thread(self._save_positions_batch, batch)
            await asyncio.to_thread(self._save_commits_batch, batch)

        return all_repositories

    async def close(self):
        await self._session.close()
        self._ch_client.close()


async def main():
    settings = Settings()  # type: ignore
    scrapper = GithubReposScrapper(
        access_token=settings.access_token,
        clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port,
        clickhouse_user=settings.clickhouse_user,
        clickhouse_password=settings.clickhouse_password,
        clickhouse_database=settings.clickhouse_database,
        max_concurrent_requests=settings.max_concurrent_requests,
        requests_per_second=settings.requests_per_second,
        batch_size=settings.batch_size,
    )

    try:
        repositories = await scrapper.get_repositories(limit=100)
        print(f"✅ Обработано и сохранено {len(repositories)} репозиториев")
        print(f"📊 Данные сохранены в 3 таблицы:")
        print(f"   - repositories: информация о репозиториях")
        print(f"   - repositories_positions: позиции в топе")
        print(f"   - repositories_authors_commits: коммиты авторов")
    finally:
        await scrapper.close()


if __name__ == "__main__":
    asyncio.run(main())
