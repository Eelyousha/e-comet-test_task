import asyncio
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from urllib.parse import urlencode

import pytest
from aioresponses import aioresponses
from main import (
    GITHUB_API_BASE_URL,
    GithubReposScrapper,
    Repository,
    RepositoryAuthorCommitsNum,
)


@pytest.fixture
def mock_github_token():
    """Фикстура для токена GitHub"""
    return "test_github_token_123"


@pytest.fixture
def mock_clickhouse_client():
    """Фикстура для мок-клиента ClickHouse"""
    client = MagicMock()
    client.insert = MagicMock()
    client.close = MagicMock()
    return client


@pytest.fixture
def mock_repo_data():
    """Фикстура с тестовыми данными репозитория"""
    return {
        "name": "test-repo",
        "owner": {"login": "test-owner"},
        "stargazers_count": 1000,
        "watchers_count": 500,
        "forks_count": 200,
        "language": "Python",
    }


@pytest.fixture
def mock_commits_data():
    """Фикстура с тестовыми данными коммитов"""
    return [
        {"author": {"login": "author1"}, "commit": {"message": "Fix bug"}},
        {"author": {"login": "author1"}, "commit": {"message": "Add feature"}},
        {"author": {"login": "author2"}, "commit": {"message": "Update docs"}},
        {
            "author": None,  # Коммит без автора
            "commit": {"message": "Automated commit"},
        },
    ]


class TestGithubReposScrapper:
    """Тесты для класса GithubReposScrapper"""

    @pytest.mark.asyncio
    async def test_initialization(self, mock_github_token, mock_clickhouse_client):
        """Тест инициализации класса"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                clickhouse_host="localhost",
                clickhouse_port=8123,
                max_concurrent_requests=10,
                requests_per_second=5,
                batch_size=50,
            )

            assert scrapper._rps_limit == 5
            assert scrapper._batch_size == 50
            assert scrapper._semaphore._value == 10

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_rate_limit(self, mock_github_token, mock_clickhouse_client):
        """Тест ограничения RPS"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=3,
                batch_size=2,
            )

            start_time = asyncio.get_event_loop().time()

            # Выполняем 5 запросов (больше лимита)
            for _ in range(5):
                await scrapper._rate_limit()

            end_time = asyncio.get_event_loop().time()
            elapsed = end_time - start_time

            # Должно пройти хотя бы 1 секунду, так как лимит 3 запроса/сек
            assert elapsed >= 0.9  # Небольшая погрешность

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_make_request_success(
        self, mock_github_token, mock_clickhouse_client
    ):
        """Тест успешного выполнения запроса"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            with aioresponses() as m:
                test_url = f"{GITHUB_API_BASE_URL}/test/endpoint"
                expected_response = {"status": "ok", "data": "test"}

                m.get(test_url, payload=expected_response)

                result = await scrapper._make_request("test/endpoint")

                assert result == expected_response

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_make_request_with_params(
        self, mock_github_token, mock_clickhouse_client
    ):
        """Тест запроса с параметрами"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            with aioresponses() as m:
                # Используем паттерн для URL с любыми параметрами
                import re

                pattern = re.compile(rf"{GITHUB_API_BASE_URL}/test/endpoint\?.*")
                expected_response = {"items": [1, 2, 3]}

                m.get(pattern, payload=expected_response)

                result = await scrapper._make_request(
                    "test/endpoint", params={"q": "test", "per_page": 10}
                )

                assert result == expected_response

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_get_top_repositories(
        self, mock_github_token, mock_clickhouse_client, mock_repo_data
    ):
        """Тест получения топ репозиториев"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            with aioresponses() as m:
                # Используем паттерн для перехвата URL с параметрами
                import re

                pattern = re.compile(rf"{GITHUB_API_BASE_URL}/search/repositories\?.*")
                response_data = {"items": [mock_repo_data, mock_repo_data]}

                m.get(pattern, payload=response_data)

                result = await scrapper._get_top_repositories(limit=2)

                assert len(result) == 2
                assert result[0]["name"] == "test-repo"

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_get_repository_commits(
        self, mock_github_token, mock_clickhouse_client, mock_commits_data
    ):
        """Тест получения коммитов репозитория"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            with aioresponses() as m:
                # Используем паттерн для перехвата URL с параметрами
                import re

                pattern = re.compile(
                    rf"{GITHUB_API_BASE_URL}/repos/test-owner/test-repo/commits\?.*"
                )

                m.get(pattern, payload=mock_commits_data)

                result = await scrapper._get_repository_commits(
                    "test-owner", "test-repo"
                )

                assert len(result) == 4
                assert result[0]["author"]["login"] == "author1"

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_get_repository_commits_empty(
        self, mock_github_token, mock_clickhouse_client
    ):
        """Тест получения коммитов для репозитория без коммитов"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            with aioresponses() as m:
                # Используем паттерн для перехвата URL с параметрами
                import re

                pattern = re.compile(
                    rf"{GITHUB_API_BASE_URL}/repos/test-owner/test-repo/commits\?.*"
                )

                m.get(pattern, payload=[])

                result = await scrapper._get_repository_commits(
                    "test-owner", "test-repo"
                )

                assert result == []

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_get_repository_commits_error_response(
        self, mock_github_token, mock_clickhouse_client
    ):
        """Тест обработки ошибочного ответа при получении коммитов"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            with aioresponses() as m:
                # Используем паттерн для перехвата URL с параметрами
                import re

                pattern = re.compile(
                    rf"{GITHUB_API_BASE_URL}/repos/test-owner/test-repo/commits\?.*"
                )

                # GitHub может вернуть объект с ошибкой вместо списка
                m.get(pattern, payload={"message": "Not Found"})

                result = await scrapper._get_repository_commits(
                    "test-owner", "test-repo"
                )

                assert result == []

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_process_repository(
        self,
        mock_github_token,
        mock_clickhouse_client,
        mock_repo_data,
        mock_commits_data,
    ):
        """Тест обработки одного репозитория"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            with aioresponses() as m:
                # Используем паттерн для перехвата URL с параметрами
                import re

                pattern = re.compile(
                    rf"{GITHUB_API_BASE_URL}/repos/test-owner/test-repo/commits\?.*"
                )

                m.get(pattern, payload=mock_commits_data)

                repo = await scrapper._process_repository(mock_repo_data, position=1)

                assert isinstance(repo, Repository)
                assert repo.name == "test-repo"
                assert repo.owner == "test-owner"
                assert repo.position == 1
                assert repo.stars == 1000
                assert repo.watchers == 500
                assert repo.forks == 200
                assert repo.language == "Python"

                # Проверяем авторов коммитов
                assert len(repo.authors_commits_num_today) == 2

                # Находим автора с наибольшим количеством коммитов
                author1_commits = next(
                    (
                        a
                        for a in repo.authors_commits_num_today
                        if a.author == "author1"
                    ),
                    None,
                )
                assert author1_commits is not None
                assert author1_commits.commits_num == 2

                author2_commits = next(
                    (
                        a
                        for a in repo.authors_commits_num_today
                        if a.author == "author2"
                    ),
                    None,
                )
                assert author2_commits is not None
                assert author2_commits.commits_num == 1

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_process_repository_no_language(
        self, mock_github_token, mock_clickhouse_client, mock_commits_data
    ):
        """Тест обработки репозитория без языка программирования"""
        repo_data = {
            "name": "test-repo",
            "owner": {"login": "test-owner"},
            "stargazers_count": 100,
            "watchers_count": 50,
            "forks_count": 20,
            "language": None,  # Язык не указан
        }

        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            with aioresponses() as m:
                # Используем паттерн для перехвата URL с параметрами
                import re

                pattern = re.compile(
                    rf"{GITHUB_API_BASE_URL}/repos/test-owner/test-repo/commits\?.*"
                )
                m.get(pattern, payload=mock_commits_data)

                repo = await scrapper._process_repository(repo_data, position=1)

                assert repo.language == "Unknown"

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_save_repositories_batch(
        self, mock_github_token, mock_clickhouse_client
    ):
        """Тест сохранения батча репозиториев"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            repositories = [
                Repository(
                    name="repo1",
                    owner="owner1",
                    position=1,
                    stars=1000,
                    watchers=500,
                    forks=200,
                    language="Python",
                    authors_commits_num_today=[],
                ),
                Repository(
                    name="repo2",
                    owner="owner2",
                    position=2,
                    stars=2000,
                    watchers=1000,
                    forks=400,
                    language="JavaScript",
                    authors_commits_num_today=[],
                ),
            ]

            scrapper._save_repositories_batch(repositories)

            # Проверяем, что insert был вызван
            mock_clickhouse_client.insert.assert_called_once()

            # Проверяем параметры вызова
            call_args = mock_clickhouse_client.insert.call_args
            assert call_args.kwargs["table"] == "repositories"
            assert len(call_args.kwargs["data"]) == 2
            assert call_args.kwargs["data"][0][0] == "repo1"  # name
            assert call_args.kwargs["data"][0][1] == "owner1"  # owner

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_save_positions_batch(
        self, mock_github_token, mock_clickhouse_client
    ):
        """Тест сохранения батча позиций"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            repositories = [
                Repository(
                    name="repo1",
                    owner="owner1",
                    position=1,
                    stars=1000,
                    watchers=500,
                    forks=200,
                    language="Python",
                    authors_commits_num_today=[],
                )
            ]

            scrapper._save_positions_batch(repositories)

            mock_clickhouse_client.insert.assert_called_once()

            call_args = mock_clickhouse_client.insert.call_args
            assert call_args.kwargs["table"] == "repositories_positions"
            assert call_args.kwargs["data"][0][1] == "owner1/repo1"  # repo

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_save_commits_batch(self, mock_github_token, mock_clickhouse_client):
        """Тест сохранения батча коммитов"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            repositories = [
                Repository(
                    name="repo1",
                    owner="owner1",
                    position=1,
                    stars=1000,
                    watchers=500,
                    forks=200,
                    language="Python",
                    authors_commits_num_today=[
                        RepositoryAuthorCommitsNum(author="author1", commits_num=5),
                        RepositoryAuthorCommitsNum(author="author2", commits_num=3),
                    ],
                )
            ]

            scrapper._save_commits_batch(repositories)

            mock_clickhouse_client.insert.assert_called_once()

            call_args = mock_clickhouse_client.insert.call_args
            assert call_args.kwargs["table"] == "repositories_authors_commits"
            assert len(call_args.kwargs["data"]) == 2
            assert call_args.kwargs["data"][0][2] == "author1"  # author
            assert call_args.kwargs["data"][0][3] == 5  # commits_num

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_save_commits_batch_empty(
        self, mock_github_token, mock_clickhouse_client
    ):
        """Тест сохранения пустого батча коммитов"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            repositories = [
                Repository(
                    name="repo1",
                    owner="owner1",
                    position=1,
                    stars=1000,
                    watchers=500,
                    forks=200,
                    language="Python",
                    authors_commits_num_today=[],  # Нет коммитов
                )
            ]

            scrapper._save_commits_batch(repositories)

            # insert не должен быть вызван для пустых данных
            mock_clickhouse_client.insert.assert_not_called()

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_get_repositories_with_batching(
        self,
        mock_github_token,
        mock_clickhouse_client,
        mock_repo_data,
        mock_commits_data,
    ):
        """Тест получения репозиториев с батчированием"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            # batch_size = 2, получаем 3 репозитория
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            with aioresponses() as m:
                import re

                # Паттерн для поиска репозиториев
                search_pattern = re.compile(
                    rf"{GITHUB_API_BASE_URL}/search/repositories\?.*"
                )
                m.get(search_pattern, payload={"items": [mock_repo_data] * 3})

                # Паттерн для запросов коммитов (будет вызван 3 раза)
                commits_pattern = re.compile(
                    rf"{GITHUB_API_BASE_URL}/repos/test-owner/test-repo/commits\?.*"
                )
                m.get(commits_pattern, payload=mock_commits_data, repeat=True)

                repositories = await scrapper.get_repositories(limit=3)

                assert len(repositories) == 3

                # Должно быть 2 батча: первый с 2 репозиториями, второй с 1
                # Для каждого батча вызывается 3 метода insert (repositories, positions, commits)
                # Итого: 2 батча * 3 таблицы = 6 вызовов
                assert mock_clickhouse_client.insert.call_count == 6

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_concurrent_requests_limit(
        self, mock_github_token, mock_clickhouse_client
    ):
        """Тест ограничения количества одновременных запросов"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=2,  # Только 2 одновременных запроса
                requests_per_second=10,
                batch_size=2,
            )

            call_count = 0
            max_concurrent = 0
            current_concurrent = 0

            async def mock_request():
                nonlocal call_count, max_concurrent, current_concurrent
                current_concurrent += 1
                max_concurrent = max(max_concurrent, current_concurrent)
                call_count += 1
                await asyncio.sleep(0.1)  # Имитируем работу
                current_concurrent -= 1

            # Запускаем 5 задач
            tasks = []
            for _ in range(5):

                async def task():
                    async with scrapper._semaphore:
                        await mock_request()

                tasks.append(task())

            await asyncio.gather(*tasks)

            assert call_count == 5
            assert max_concurrent <= 2  # Не более 2 одновременно

            await scrapper.close()

    @pytest.mark.asyncio
    async def test_close(self, mock_github_token, mock_clickhouse_client):
        """Тест закрытия соединений"""
        with patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_client
        ):
            scrapper = GithubReposScrapper(
                access_token=mock_github_token,
                max_concurrent_requests=5,
                requests_per_second=10,
                batch_size=2,
            )

            await scrapper.close()

            # Проверяем, что сессия закрыта
            assert scrapper._session.closed

            # Проверяем, что ClickHouse клиент закрыт
            mock_clickhouse_client.close.assert_called_once()


class TestRepositoryDataClass:
    """Тесты для dataclass Repository"""

    def test_repository_creation(self):
        """Тест создания экземпляра Repository"""
        repo = Repository(
            name="test-repo",
            owner="test-owner",
            position=1,
            stars=1000,
            watchers=500,
            forks=200,
            language="Python",
            authors_commits_num_today=[],
        )

        assert repo.name == "test-repo"
        assert repo.owner == "test-owner"
        assert repo.position == 1
        assert isinstance(repo.authors_commits_num_today, list)

    def test_repository_with_commits(self):
        """Тест Repository с коммитами"""
        commits = [
            RepositoryAuthorCommitsNum(author="user1", commits_num=5),
            RepositoryAuthorCommitsNum(author="user2", commits_num=3),
        ]

        repo = Repository(
            name="test-repo",
            owner="test-owner",
            position=1,
            stars=1000,
            watchers=500,
            forks=200,
            language="Python",
            authors_commits_num_today=commits,
        )

        assert len(repo.authors_commits_num_today) == 2
        assert repo.authors_commits_num_today[0].author == "user1"
        assert repo.authors_commits_num_today[0].commits_num == 5


class TestRepositoryAuthorCommitsNumDataClass:
    """Тесты для dataclass RepositoryAuthorCommitsNum"""

    def test_author_commits_creation(self):
        """Тест создания экземпляра RepositoryAuthorCommitsNum"""
        author_commits = RepositoryAuthorCommitsNum(author="test-user", commits_num=10)

        assert author_commits.author == "test-user"
        assert author_commits.commits_num == 10
