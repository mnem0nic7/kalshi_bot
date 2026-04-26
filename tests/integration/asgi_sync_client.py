from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any

import httpx

from kalshi_bot.services.container import AppContainer


class SameThreadASGITestClient:
    """Minimal sync facade over ASGITransport for this sandbox.

    Starlette's TestClient drives the app through a cross-thread AnyIO portal.
    The current test runtime can lose cross-thread event-loop wakeups, so web
    integration tests use this same-thread client instead.
    """

    __test__ = False

    def __init__(self, app: Any, *, base_url: str = "http://testserver") -> None:
        self.app = app
        self.base_url = base_url
        self.cookies = httpx.Cookies()
        self._container: AppContainer | None = None

    def __enter__(self) -> "SameThreadASGITestClient":
        self._container = asyncio.run(AppContainer.build())
        self.app.state.container = self._container
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._container is not None:
            asyncio.run(self._container.close())
            self._container = None

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        return asyncio.run(self._request(method, url, **kwargs))

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        transport = httpx.ASGITransport(app=self.app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url=self.base_url,
            cookies=self.cookies,
            follow_redirects=False,
        ) as client:
            response = await client.request(method, url, **kwargs)
            self.cookies.update(client.cookies)
            return response
