from datetime import date
from unittest.mock import MagicMock

import pytest

from models import Repository, RepositoryAuthorCommitsNum
from storage import ClickhouseStorage


@pytest.fixture
def mock_ch_client():
    client = MagicMock()
    client.insert = MagicMock()
    client.close = MagicMock()
    return client


@pytest.fixture
def storage(mock_ch_client, monkeypatch):
    monkeypatch.setattr("clickhouse_connect.get_client", lambda **_: mock_ch_client)
    return ClickhouseStorage()


@pytest.fixture
def sample_repositories():
    return [
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
        ),
        Repository(
            name="repo2",
            owner="owner2",
            position=2,
            stars=2000,
            watchers=1000,
            forks=400,
            language="JavaScript",
            authors_commits_num_today=[
                RepositoryAuthorCommitsNum(author="author3", commits_num=2),
            ],
        ),
    ]


class TestSaveRepositories:
    def test_inserts_correct_table(self, storage, mock_ch_client, sample_repositories):
        storage._save_repositories(sample_repositories)

        call_args = mock_ch_client.insert.call_args
        assert call_args.kwargs["table"] == "repositories"

    def test_inserts_correct_row_count(self, storage, mock_ch_client, sample_repositories):
        storage._save_repositories(sample_repositories)

        call_args = mock_ch_client.insert.call_args
        assert len(call_args.kwargs["data"]) == 2

    def test_inserts_correct_fields(self, storage, mock_ch_client, sample_repositories):
        storage._save_repositories(sample_repositories)

        row = mock_ch_client.insert.call_args.kwargs["data"][0]
        assert row[0] == "repo1"   # name
        assert row[1] == "owner1"  # owner
        assert row[2] == 1000      # stars
        assert row[3] == 500       # watchers
        assert row[4] == 200       # forks
        assert row[5] == "Python"  # language


class TestSavePositions:
    def test_inserts_correct_table(self, storage, mock_ch_client, sample_repositories):
        storage._save_positions(sample_repositories)

        call_args = mock_ch_client.insert.call_args
        assert call_args.kwargs["table"] == "repositories_positions"

    def test_inserts_correct_row_count(self, storage, mock_ch_client, sample_repositories):
        storage._save_positions(sample_repositories)

        call_args = mock_ch_client.insert.call_args
        assert len(call_args.kwargs["data"]) == 2

    def test_repo_full_name_format(self, storage, mock_ch_client, sample_repositories):
        storage._save_positions(sample_repositories)

        rows = mock_ch_client.insert.call_args.kwargs["data"]
        assert rows[0][1] == "owner1/repo1"
        assert rows[1][1] == "owner2/repo2"

    def test_positions_correct(self, storage, mock_ch_client, sample_repositories):
        storage._save_positions(sample_repositories)

        rows = mock_ch_client.insert.call_args.kwargs["data"]
        assert rows[0][2] == 1
        assert rows[1][2] == 2

    def test_date_is_today(self, storage, mock_ch_client, sample_repositories):
        storage._save_positions(sample_repositories)

        rows = mock_ch_client.insert.call_args.kwargs["data"]
        assert rows[0][0] == date.today()


class TestSaveCommits:
    def test_inserts_correct_table(self, storage, mock_ch_client, sample_repositories):
        storage._save_commits(sample_repositories)

        call_args = mock_ch_client.insert.call_args
        assert call_args.kwargs["table"] == "repositories_authors_commits"

    def test_inserts_correct_row_count(self, storage, mock_ch_client, sample_repositories):
        # repo1 имеет 2 автора, repo2 — 1, итого 3 строки
        storage._save_commits(sample_repositories)

        rows = mock_ch_client.insert.call_args.kwargs["data"]
        assert len(rows) == 3

    def test_inserts_correct_fields(self, storage, mock_ch_client, sample_repositories):
        storage._save_commits(sample_repositories)

        rows = mock_ch_client.insert.call_args.kwargs["data"]
        assert rows[0][1] == "owner1/repo1"  # repo
        assert rows[0][2] == "author1"       # author
        assert rows[0][3] == 5               # commits_num

    def test_no_insert_when_no_commits(self, storage, mock_ch_client):
        repositories = [
            Repository(
                name="repo1", owner="owner1", position=1,
                stars=0, watchers=0, forks=0, language="Python",
                authors_commits_num_today=[],
            )
        ]

        storage._save_commits(repositories)

        mock_ch_client.insert.assert_not_called()


class TestSaveBatch:
    def test_calls_all_three_tables(self, storage, mock_ch_client, sample_repositories):
        storage.save_batch(sample_repositories)

        tables = [call.kwargs["table"] for call in mock_ch_client.insert.call_args_list]
        assert "repositories" in tables
        assert "repositories_positions" in tables
        assert "repositories_authors_commits" in tables

    def test_empty_batch_does_nothing(self, storage, mock_ch_client):
        storage.save_batch([])

        mock_ch_client.insert.assert_not_called()

    def test_insert_order(self, storage, mock_ch_client, sample_repositories):
        """Репозитории вставляются раньше позиций и коммитов"""
        storage.save_batch(sample_repositories)

        tables = [call.kwargs["table"] for call in mock_ch_client.insert.call_args_list]
        assert tables[0] == "repositories"
        assert tables[1] == "repositories_positions"
        assert tables[2] == "repositories_authors_commits"


class TestClose:
    def test_close_calls_client(self, storage, mock_ch_client):
        storage.close()

        mock_ch_client.close.assert_called_once()
