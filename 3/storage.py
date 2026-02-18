from datetime import date, datetime, timezone

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from models import Repository


class ClickhouseStorage:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        user: str = "default",
        password: str = "",
        database: str = "test",
    ):
        self._client: Client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
        )

    def close(self) -> None:
        self._client.close()

    def _save_repositories(self, repositories: list[Repository]) -> None:
        """Сохраняет репозитории в таблицу repositories"""
        current_time = datetime.now(timezone.utc)

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

        self._client.insert(
            table="repositories",
            data=repo_data,
            column_names=["name", "owner", "stars", "watchers", "forks", "language", "updated"],
        )

    def _save_positions(self, repositories: list[Repository]) -> None:
        """Сохраняет позиции репозиториев в таблицу repositories_positions"""
        current_date = date.today()

        positions_data = [
            [current_date, f"{repo.owner}/{repo.name}", repo.position]
            for repo in repositories
        ]

        self._client.insert(
            table="repositories_positions",
            data=positions_data,
            column_names=["date", "repo", "position"],
        )

    def _save_commits(self, repositories: list[Repository]) -> None:
        """Сохраняет данные о коммитах в таблицу repositories_authors_commits"""
        current_date = date.today()

        commits_data = []
        for repo in repositories:
            repo_full_name = f"{repo.owner}/{repo.name}"
            for author_commit in repo.authors_commits_num_today:
                commits_data.append(
                    [current_date, repo_full_name, author_commit.author, author_commit.commits_num]
                )

        if not commits_data:
            return

        self._client.insert(
            table="repositories_authors_commits",
            data=commits_data,
            column_names=["date", "repo", "author", "commits_num"],
        )

    def save_batch(self, repositories: list[Repository]) -> None:
        """Сохраняет батч репозиториев во все три таблицы"""
        if not repositories:
            return

        self._save_repositories(repositories)
        self._save_positions(repositories)
        self._save_commits(repositories)
