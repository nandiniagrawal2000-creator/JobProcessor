"""Tests for env-driven Settings (pydantic-settings)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.config import Settings


def test_defaults():
    settings = Settings()
    assert settings.max_workers == 4
    assert settings.queue_size == 1000
    assert settings.request_timeout_seconds == 30.0
    assert settings.max_retries == 3


def test_env_vars_override_defaults(monkeypatch):
    monkeypatch.setenv("JOB_MAX_WORKERS", "9")
    monkeypatch.setenv("JOB_REQUEST_TIMEOUT_SECONDS", "7.5")
    monkeypatch.setenv("JOB_MAX_RETRIES", "5")

    settings = Settings()
    assert settings.max_workers == 9
    assert settings.request_timeout_seconds == 7.5
    assert settings.max_retries == 5


def test_invalid_value_fails_fast(monkeypatch):
    monkeypatch.setenv("JOB_MAX_WORKERS", "0")  # violates ge=1
    with pytest.raises(ValidationError):
        Settings()
