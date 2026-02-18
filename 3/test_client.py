import asyncio
import re

import pytest
from aioresponses import aioresponses

from client import GITHUB_API_BASE_URL, GithubClient


@pytest.fixture
def access_token():
    return "test_github_token_123"


@pytest.fixture
def client(access_token):
    return GithubClient(
        access_token=access_token,
        max_concurrent_requests=5,
        requests_per_second=10,
    )


class TestGithubClientContextManager:
    @pytest.mark.asyncio
    async def test_session_created_on_enter(self, client):
        assert client._session is None

        async with client:
            assert client._session is not None
            assert not client._session.closed

    @pytest.mark.asyncio
    async def test_session_closed_on_exit(self, client):
        async with client:
            pass

        assert client._session.closed

    @pytest.mark.asyncio
    async def test_session_closed_on_exception(self, client):
        with pytest.raises(RuntimeError):
            async with client:
                raise RuntimeError("test error")

        assert client._session.closed

    @pytest.mark.asyncio
    async def test_make_request_without_context_manager_raises(self, client):
        with pytest.raises(AssertionError):
            await client.make_request("test/endpoint")


class TestGithubClientRateLimit:
    @pytest.mark.asyncio
    async def test_rate_limit_allows_requests_under_limit(self, client):
        async with client:
            start = asyncio.get_event_loop().time()

            for _ in range(5):
                await client._rate_limit()

            elapsed = asyncio.get_event_loop().time() - start

            # 5 запросов при лимите 10 RPS не должны вызвать задержку
            assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_rate_limit_delays_when_exceeded(self):
        client = GithubClient(
            access_token="token",
            max_concurrent_requests=5,
            requests_per_second=3,
        )
        async with client:
            start = asyncio.get_event_loop().time()

            for _ in range(5):
                await client._rate_limit()

            elapsed = asyncio.get_event_loop().time() - start

            # 5 запросов при лимите 3 RPS должны занять >= 1 секунды
            assert elapsed >= 0.9


class TestGithubClientMakeRequest:
    @pytest.mark.asyncio
    async def test_make_request_success(self, client):
        async with client:
            with aioresponses() as m:
                expected = {"status": "ok"}
                m.get(f"{GITHUB_API_BASE_URL}/test/endpoint", payload=expected)

                result = await client.make_request("test/endpoint")

                assert result == expected

    @pytest.mark.asyncio
    async def test_make_request_with_params(self, client):
        async with client:
            with aioresponses() as m:
                pattern = re.compile(rf"{GITHUB_API_BASE_URL}/test/endpoint\?.*")
                expected = {"items": [1, 2, 3]}
                m.get(pattern, payload=expected)

                result = await client.make_request(
                    "test/endpoint", params={"q": "test", "per_page": 10}
                )

                assert result == expected

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrent_requests(self):
        client = GithubClient(
            access_token="token",
            max_concurrent_requests=2,
            requests_per_second=100,
        )

        max_concurrent = 0
        current_concurrent = 0

        async def mock_task():
            nonlocal max_concurrent, current_concurrent
            async with client._semaphore:
                current_concurrent += 1
                max_concurrent = max(max_concurrent, current_concurrent)
                await asyncio.sleep(0.05)
                current_concurrent -= 1

        async with client:
            await asyncio.gather(*[mock_task() for _ in range(6)])

        assert max_concurrent <= 2

    @pytest.mark.asyncio
    async def test_authorization_header_sent(self, access_token):
        client = GithubClient(access_token=access_token)

        async with client:
            assert client._session is not None
            assert client._session.headers["Authorization"] == f"Bearer {access_token}"