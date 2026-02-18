import pytest
from models import Repository, RepositoryAuthorCommitsNum


class TestRepositoryAuthorCommitsNum:
    def test_creation(self):
        author_commits = RepositoryAuthorCommitsNum(author="test-user", commits_num=10)

        assert author_commits.author == "test-user"
        assert author_commits.commits_num == 10


class TestRepository:
    def test_creation(self):
        repo = Repository(
            name="test-repo",
            owner="test-owner",
            position=1,
            stars=1000,
            watchers=500,
            forks=200,
            language="Python",
        )

        assert repo.name == "test-repo"
        assert repo.owner == "test-owner"
        assert repo.position == 1
        assert repo.authors_commits_num_today == []

    def test_creation_with_commits(self):
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

    def test_default_authors_commits_not_shared(self):
        """Проверяем что default_factory создаёт новый список для каждого экземпляра"""
        repo1 = Repository(name="r1", owner="o", position=1, stars=0, watchers=0, forks=0, language="Python")
        repo2 = Repository(name="r2", owner="o", position=2, stars=0, watchers=0, forks=0, language="Python")

        repo1.authors_commits_num_today.append(RepositoryAuthorCommitsNum("user", 1))

        assert len(repo2.authors_commits_num_today) == 0
