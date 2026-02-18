import asyncio
from typing import Any, Final

from aiohttp import ClientSession

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"


class GithubClient:
    def __init__(
        self,
        access_token: str,
        max_concurrent_requests: int = 10,
        requests_per_second: int = 10,
    ):
        self._access_token = access_token
        self._session: ClientSession | None = None

        # Семафор для ограничения максимального количества одновременных запросов
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)

        # Для ограничения RPS
        self._rps_limit = requests_per_second
        self._request_times: list[float] = []
        self._rps_lock = asyncio.Lock()

    async def __aenter__(self) -> "GithubClient":
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {self._access_token}",
            }
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._session:
            await self._session.close()

    async def _rate_limit(self) -> None:
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

    async def make_request(
        self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None
    ) -> Any:
        assert self._session is not None, "Session is not initialized. Use async context manager."

        async with self._semaphore:
            await self._rate_limit()
            async with self._session.request(
                method, f"{GITHUB_API_BASE_URL}/{endpoint}", params=params
            ) as response:
                return await response.json()
