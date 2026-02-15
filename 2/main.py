import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from aiohttp import ClientSession

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
        max_concurrent_requests: int = 10,
        requests_per_second: int = 10,
    ):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )

        self._semaphore = asyncio.Semaphore(max_concurrent_requests)

        self._rps_limit = requests_per_second
        self._request_times: list[float] = []
        self._rps_lock = asyncio.Lock()

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

    async def get_repositories(self, limit: int = 100) -> list[Repository]:
        """Получает топ репозиториев с информацией о коммитах за последний день"""
        # Получаем топ репозиториев
        top_repos = await self._get_top_repositories(limit)

        # Асинхронно обрабатываем все репозитории
        tasks = [
            self._process_repository(repo_data, position=idx + 1)
            for idx, repo_data in enumerate(top_repos)
        ]

        # Ждём завершения всех задач
        repositories = await asyncio.gather(*tasks)

        return repositories

    async def close(self):
        await self._session.close()
