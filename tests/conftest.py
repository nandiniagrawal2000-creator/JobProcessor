"""Shared pytest fixtures."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import JobStore
from app.store import store as global_store


@pytest.fixture(autouse=True)
def instant_sleep(monkeypatch):
    """Make retry backoff instant in tests (patch the isolated `_sleep`
    indirection, NOT the global asyncio.sleep). Yields the mock so tests can
    assert how many times / with what delays a retry waited."""
    sleep_mock = AsyncMock()
    monkeypatch.setattr("app.jobs._sleep", sleep_mock)
    return sleep_mock


@pytest.fixture(autouse=True)
def no_simulated_work(monkeypatch):
    """Skip the simulated processing delay in tests. Yields the mock so tests
    can assert it ran (and with what duration)."""
    work_mock = AsyncMock()
    monkeypatch.setattr("app.jobs._simulate_work", work_mock)
    return work_mock


@pytest.fixture(autouse=True)
def reset_store():
    """The app uses a single global in-memory store. Clear it around every test
    so tests are independent and order doesn't matter."""
    global_store._jobs.clear()
    yield
    global_store._jobs.clear()


@pytest.fixture
def client() -> TestClient:
    """A synchronous TestClient. Note: Starlette's TestClient runs a request's
    BackgroundTasks to completion *before* the `client.post(...)` call returns,
    so after POST /jobs the background `process_job` has already finished."""
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def store() -> JobStore:
    """A fresh, isolated JobStore for unit-testing the worker directly."""
    return JobStore()
