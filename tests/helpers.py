"""Helpers for mocking the external HTTP call in tests.

`process_job` talks to the outside world through:

    async with httpx.AsyncClient(...) as client:
        response = await client.get(url)
        response.raise_for_status()

To keep tests fast and offline, we replace `httpx.AsyncClient` (as imported in
`app.jobs`) with a MagicMock factory that returns an async-context-manager whose
`client.get(...)` is an AsyncMock. We can then hand back a fake Response, or make
`.get` raise to simulate timeouts / connection errors.
"""

from __future__ import annotations

import time
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import httpx


def make_response(
    status_code: int = 200,
    *,
    json_data: Optional[Any] = None,
    text: Optional[str] = None,
    headers: Optional[dict[str, str]] = None,
    raise_for_status_exc: Optional[Exception] = None,
) -> MagicMock:
    """Build a fake httpx.Response.

    - If `json_data` is given, `response.json()` returns it.
    - Otherwise `response.json()` raises ValueError (simulating a non-JSON body)
      and `response.text` returns `text`, so we can test the text fallback.
    - If `raise_for_status_exc` is given, `raise_for_status()` raises it
      (simulating a 4xx/5xx from the external API).
    """
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = headers if headers is not None else {"content-type": "application/json"}

    if json_data is not None:
        resp.json.return_value = json_data
    else:
        resp.json.side_effect = ValueError("not json")
        resp.text = text if text is not None else ""

    if raise_for_status_exc is not None:
        resp.raise_for_status.side_effect = raise_for_status_exc
    else:
        resp.raise_for_status.return_value = None

    return resp


def patch_async_client(
    monkeypatch,
    *,
    response: Optional[MagicMock] = None,
    get_side_effect: Optional[Any] = None,
) -> AsyncMock:
    """Patch `app.jobs.httpx.AsyncClient` and return the mocked client.

    Provide EITHER `response` (what `client.get` resolves to) OR
    `get_side_effect` (an exception instance/class that `client.get` raises).
    """
    client_mock = AsyncMock()
    if get_side_effect is not None:
        client_mock.get.side_effect = get_side_effect
    else:
        client_mock.get.return_value = response

    # httpx.AsyncClient(...) is used as `async with ... as client`.
    async_cm = MagicMock()
    async_cm.__aenter__ = AsyncMock(return_value=client_mock)
    async_cm.__aexit__ = AsyncMock(return_value=False)

    factory = MagicMock(return_value=async_cm)
    monkeypatch.setattr("app.jobs.httpx.AsyncClient", factory)
    return client_mock


def wait_for_terminal(client, job_id: str, timeout: float = 3.0) -> dict:
    """Poll GET /job/{id} until the job reaches Completed/Failed.

    Processing is now asynchronous (queue + worker pool), so unlike the old
    BackgroundTasks flow the job is usually still Queued/Running right after the
    POST returns. The TestClient runs the app's event loop in a background
    thread, so workers keep making progress while we sleep here.
    """
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        response = client.get(f"/job/{job_id}")
        if response.status_code == 200:
            last = response.json()
            if last["status"] in ("Completed", "Failed"):
                return last
        time.sleep(0.02)
    raise AssertionError(f"Job {job_id} did not settle within {timeout}s: {last}")


def http_status_error(status_code: int) -> httpx.HTTPStatusError:
    """Build a real HTTPStatusError carrying a response with `status_code`."""
    request = httpx.Request("GET", "http://external.test/api")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(
        f"HTTP {status_code}", request=request, response=response
    )
