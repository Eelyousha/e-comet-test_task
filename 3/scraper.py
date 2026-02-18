import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from client import GithubClient
from models import Repository, RepositoryAuthorCommitsNum
from storage import ClickhouseStorage


class GithubReposScrapper:
    def __init__(
        self,
        client: GithubClient,
        storage: ClickhouseStorage,
        batch_size: int = 50,
    ):
        self._client = client
        self._storage = storage
        self._batch_size = batch_size

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories"""
        data = await self._client.make_request(
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
        since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        data = await self._client.make_request(
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

        commits = await self._get_repository_commits(owner, name)

        author_commits: dict[str, int] = {}
        for commit in commits:
            if commit.get("author") and commit["author"].get("login"):
                author = commit["author"]["login"]
                author_commits[author] = author_commits.get(author, 0) + 1

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
        """Получает топ репозиториев с информацией о коммитах за последний день и сохраняет в ClickHouse"""
        top_repos = await self._get_top_repositories(limit)
        all_repositories: list[Repository] = []

        for batch_start in range(0, len(top_repos), self._batch_size):
            chunk = top_repos[batch_start : batch_start + self._batch_size]

            results = await asyncio.gather(
                *[
                    self._process_repository(repo_data, position=batch_start + idx + 1)
                    for idx, repo_data in enumerate(chunk)
                ],
                return_exceptions=True,
            )

            batch: list[Repository] = []
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    repo_name = chunk[idx].get("full_name", "unknown")
                    print(f"⚠️ Ошибка при обработке {repo_name}: {result}")
                else:
                    assert isinstance(result, Repository)
                    batch.append(result)

            if batch:
                await asyncio.to_thread(self._storage.save_batch, batch)
                all_repositories.extend(batch)

        return all_repositories
