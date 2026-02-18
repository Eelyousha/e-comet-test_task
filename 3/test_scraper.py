import asyncio
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aioresponses import aioresponses

from client import GITHUB_API_BASE_URL, GithubClient
from models import Repository, RepositoryAuthorCommitsNum
from scraper import GithubReposScrapper
from storage import ClickhouseStorage


@pytest.fixture
def mock_client():
    client = MagicMock(spec=GithubClient)
    client.make_request = AsyncMock()
    return client


@pytest.fixture
def mock_storage():
    storage = MagicMock(spec=ClickhouseStorage)
    storage.save_batch = MagicMock()
    return storage


@pytest.fixture
def scraper(mock_client, mock_storage):
    return GithubReposScrapper(client=mock_client, storage=mock_storage, batch_size=2)


@pytest.fixture
def mock_repo_data():
    return {
        "name": "test-repo",
        "full_name": "test-owner/test-repo",
        "owner": {"login": "test-owner"},
        "stargazers_count": 1000,
        "watchers_count": 500,
        "forks_count": 200,
        "language": "Python",
    }


@pytest.fixture
def mock_commits_data():
    return [
        {"author": {"login": "author1"}, "commit": {"message": "Fix bug"}},
        {"author": {"login": "author1"}, "commit": {"message": "Add feature"}},
        {"author": {"login": "author2"}, "commit": {"message": "Update docs"}},
        {"author": None, "commit": {"message": "Automated commit"}},
    ]


class TestGetTopRepositories:
    @pytest.mark.asyncio
    async def test_returns_items(self, scraper, mock_client, mock_repo_data):
        mock_client.make_request.return_value = {"items": [mock_repo_data] * 3}

        result = await scraper._get_top_repositories(limit=3)

        assert len(result) == 3
        assert result[0]["name"] == "test-repo"

    @pytest.mark.asyncio
    async def test_passes_correct_params(self, scraper, mock_client, mock_repo_data):
        mock_client.make_request.return_value = {"items": [mock_repo_data]}

        await scraper._get_top_repositories(limit=50)

        mock_client.make_request.assert_called_once_with(
            endpoint="search/repositories",
            params={"q": "stars:>1", "sort": "stars", "order": "desc", "per_page": 50},
        )


class TestGetRepositoryCommits:
    @pytest.mark.asyncio
    async def test_returns_commits_list(self, scraper, mock_client, mock_commits_data):
        mock_client.make_request.return_value = mock_commits_data

        result = await scraper._get_repository_commits("test-owner", "test-repo")

        assert len(result) == 4

    @pytest.mark.asyncio
    async def test_returns_empty_list_on_error_response(self, scraper, mock_client):
        mock_client.make_request.return_value = {"message": "Not Found"}

        result = await scraper._get_repository_commits("test-owner", "test-repo")

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_commits(self, scraper, mock_client):
        mock_client.make_request.return_value = []

        result = await scraper._get_repository_commits("test-owner", "test-repo")

        assert result == []

    @pytest.mark.asyncio
    async def test_passes_correct_endpoint(self, scraper, mock_client):
        mock_client.make_request.return_value = []

        await scraper._get_repository_commits("my-owner", "my-repo")

        call_kwargs = mock_client.make_request.call_args
        assert call_kwargs.kwargs["endpoint"] == "repos/my-owner/my-repo/commits"


class TestProcessRepository:
    @pytest.mark.asyncio
    async def test_returns_repository(self, scraper, mock_client, mock_repo_data, mock_commits_data):
        mock_client.make_request.return_value = mock_commits_data

        repo = await scraper._process_repository(mock_repo_data, position=1)

        assert isinstance(repo, Repository)
        assert repo.name == "test-repo"
        assert repo.owner == "test-owner"
        assert repo.position == 1
        assert repo.stars == 1000
        assert repo.watchers == 500
        assert repo.forks == 200
        assert repo.language == "Python"

    @pytest.mark.asyncio
    async def test_counts_commits_per_author(self, scraper, mock_client, mock_repo_data, mock_commits_data):
        mock_client.make_request.return_value = mock_commits_data

        repo = await scraper._process_repository(mock_repo_data, position=1)

        author1 = next(a for a in repo.authors_commits_num_today if a.author == "author1")
        author2 = next(a for a in repo.authors_commits_num_today if a.author == "author2")

        assert author1.commits_num == 2
        assert author2.commits_num == 1

    @pytest.mark.asyncio
    async def test_skips_commits_without_author(self, scraper, mock_client, mock_repo_data, mock_commits_data):
        mock_client.make_request.return_value = mock_commits_data

        repo = await scraper._process_repository(mock_repo_data, position=1)

        # 4 коммита в фикстуре, 1 без автора — должно быть 2 уникальных автора
        assert len(repo.authors_commits_num_today) == 2

    @pytest.mark.asyncio
    async def test_unknown_language_when_none(self, scraper, mock_client, mock_commits_data):
        repo_data = {
            "name": "test-repo",
            "full_name": "test-owner/test-repo",
            "owner": {"login": "test-owner"},
            "stargazers_count": 100,
            "watchers_count": 50,
            "forks_count": 20,
            "language": None,
        }
        mock_client.make_request.return_value = mock_commits_data

        repo = await scraper._process_repository(repo_data, position=1)

        assert repo.language == "Unknown"

    @pytest.mark.asyncio
    async def test_empty_authors_when_no_commits(self, scraper, mock_client, mock_repo_data):
        mock_client.make_request.return_value = []

        repo = await scraper._process_repository(mock_repo_data, position=1)

        assert repo.authors_commits_num_today == []


class TestGetRepositories:
    @pytest.mark.asyncio
    async def test_returns_all_repositories(self, scraper, mock_client, mock_repo_data, mock_commits_data):
        mock_client.make_request.side_effect = [
            {"items": [mock_repo_data] * 3},  # _get_top_repositories
            mock_commits_data,                 # commits для repo 1
            mock_commits_data,                 # commits для repo 2
            mock_commits_data,                 # commits для repo 3
        ]

        result = await scraper.get_repositories(limit=3)

        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_positions_are_sequential(self, scraper, mock_client, mock_repo_data, mock_commits_data):
        mock_client.make_request.side_effect = [
            {"items": [mock_repo_data] * 3},
            mock_commits_data,
            mock_commits_data,
            mock_commits_data,
        ]

        result = await scraper.get_repositories(limit=3)

        assert [r.position for r in result] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_save_batch_called_per_chunk(self, scraper, mock_client, mock_storage, mock_repo_data, mock_commits_data):
        # batch_size=2, 3 репозитория → 2 батча
        mock_client.make_request.side_effect = [
            {"items": [mock_repo_data] * 3},
            mock_commits_data,
            mock_commits_data,
            mock_commits_data,
        ]

        await scraper.get_repositories(limit=3)

        assert mock_storage.save_batch.call_count == 2

    @pytest.mark.asyncio
    async def test_failed_repository_is_skipped(self, scraper, mock_client, mock_storage, mock_repo_data, mock_commits_data):
        """Репозиторий упавший в gather не попадает в результат и не блокирует остальные"""
        async def make_request_side_effect(endpoint, **kwargs):
            if "search" in endpoint:
                return {"items": [mock_repo_data] * 3}
            if "repo2" in endpoint:
                raise RuntimeError("API error")
            return mock_commits_data

        # Патчим _get_repository_commits напрямую чтобы симулировать падение одного
        call_count = 0

        async def mock_get_commits(owner, repo):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("API error for repo 2")
            return mock_commits_data

        mock_client.make_request.return_value = {"items": [mock_repo_data] * 3}
        scraper._get_repository_commits = mock_get_commits

        result = await scraper.get_repositories(limit=3)

        # 1 упал, 2 успешных
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_failed_repository_does_not_prevent_save(self, scraper, mock_client, mock_storage, mock_repo_data, mock_commits_data):
        """Если хоть один репозиторий в батче успешен — save_batch вызывается"""
        call_count = 0

        async def mock_get_commits(owner, repo):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("API error")
            return mock_commits_data

        mock_client.make_request.return_value = {"items": [mock_repo_data] * 2}
        scraper._get_repository_commits = mock_get_commits

        await scraper.get_repositories(limit=2)

        mock_storage.save_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_batch_not_saved(self, scraper, mock_client, mock_storage, mock_repo_data):
        """Если все репозитории в батче упали — save_batch не вызывается"""
        async def mock_get_commits(owner, repo):
            raise RuntimeError("All failed")

        mock_client.make_request.return_value = {"items": [mock_repo_data] * 2}
        scraper._get_repository_commits = mock_get_commits

        await scraper.get_repositories(limit=2)

        mock_storage.save_batch.assert_not_called()
