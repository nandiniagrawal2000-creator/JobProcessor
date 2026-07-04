"""Shared pytest fixtures."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import JobStore
from app.store import store as global_store


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
