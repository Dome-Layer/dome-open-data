import asyncio
import logging
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)

USER_AGENT = "dome-open-data/0.1 (+https://github.com/Dome-Layer/dome-open-data)"
RETRY_STATUSES = {429, 500, 502, 503, 504}


class SourceError(RuntimeError):
    pass


class RateLimiter:
    def __init__(self, per_second: float) -> None:
        self._interval = 1.0 / per_second
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if self._next > now:
                await asyncio.sleep(self._next - now)
            self._next = max(now, self._next) + self._interval


def new_client(transport: httpx.AsyncBaseTransport | None = None) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=httpx.Timeout(90.0, connect=20.0),
        follow_redirects=True,
        transport=transport,
    )


async def get(
    client: httpx.AsyncClient,
    url: str,
    limiter: RateLimiter,
    params: dict[str, Any] | None = None,
    attempts: int = 4,
    backoff: float = 2.0,
) -> httpx.Response:
    for attempt in range(1, attempts + 1):
        await limiter.wait()
        try:
            response = await client.get(url, params=params)
        except httpx.TransportError as exc:
            reason = f"{type(exc).__name__}: {exc}"
        else:
            if response.status_code not in RETRY_STATUSES:
                if response.is_error:
                    raise SourceError(f"GET {response.url} returned {response.status_code}")
                return response
            reason = f"status {response.status_code}"
        if attempt == attempts:
            raise SourceError(f"GET {url} failed after {attempts} attempts ({reason})")
        delay = backoff * 2 ** (attempt - 1)
        log.warning("retrying request", extra={"url": url, "attempt": attempt, "reason": reason})
        await asyncio.sleep(delay)
    raise AssertionError("unreachable")


def require_json(response: httpx.Response) -> Any:
    # A bot challenge or error page can arrive as 200 text/html; never store it as data.
    content_type = response.headers.get("content-type", "")
    if "json" not in content_type:
        raise SourceError(
            f"GET {response.url} returned {content_type or 'no content type'}, not JSON"
        )
    try:
        return response.json()
    except ValueError as exc:
        raise SourceError(f"GET {response.url} returned invalid JSON") from exc
