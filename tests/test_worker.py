"""Unit tests for the background worker `process_job`.

These test the worker in isolation (no HTTP server): we call `process_job`
directly against a fresh JobStore and a mocked httpx client, then inspect the
stored job to assert the status transitions and captured data are correct.
"""

from __future__ import annotations

import uuid

import httpx
import pytest

from app import jobs
from app.models import Job, JobStatus
from tests.helpers import http_status_error, make_response, patch_async_client


async def _queue_job(store, url: str = "http://external.test/api") -> uuid.UUID:
    """Create a Queued job in the store and return its id."""
    job = Job(target_url=url)
    await store.add(job)
    return job.id


async def test_success_marks_completed_with_json_result(store, monkeypatch):
    patch_async_client(
        monkeypatch,
        response=make_response(
            200,
            json_data={"answer": 42},
            headers={"content-type": "application/json", "x-trace": "abc"},
        ),
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store)

    job = await store.get(job_id)
    assert job.status == JobStatus.COMPLETED
    assert job.status_code == 200
    assert job.result == {"answer": 42}
    assert job.response_headers == {"content-type": "application/json", "x-trace": "abc"}
    assert job.error is None
    assert job.updated_at >= job.created_at


async def test_success_with_non_json_body_falls_back_to_text(store, monkeypatch):
    patch_async_client(
        monkeypatch,
        response=make_response(
            200, text="plain hello", headers={"content-type": "text/plain"}
        ),
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store)

    job = await store.get(job_id)
    assert job.status == JobStatus.COMPLETED
    assert job.result == "plain hello"


async def test_status_is_running_during_external_call(store, monkeypatch):
    """While the external call is in flight, the job must already be Running."""
    observed = {}

    async def fake_get(url):
        snapshot = await store.get(job_id)
        observed["status"] = snapshot.status
        return make_response(200, json_data={})

    patch_async_client(monkeypatch, get_side_effect=fake_get)
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store)

    assert observed["status"] == JobStatus.RUNNING
    assert (await store.get(job_id)).status == JobStatus.COMPLETED


@pytest.mark.parametrize("code", [400, 404, 500, 503])
async def test_http_error_status_marks_failed(store, monkeypatch, code):
    patch_async_client(
        monkeypatch,
        response=make_response(200, json_data={}, raise_for_status_exc=http_status_error(code)),
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store)

    job = await store.get(job_id)
    assert job.status == JobStatus.FAILED
    assert job.status_code == code
    assert str(code) in job.error
    assert job.result is None


async def test_timeout_marks_failed(store, monkeypatch):
    patch_async_client(
        monkeypatch, get_side_effect=httpx.TimeoutException("request timed out")
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store)

    job = await store.get(job_id)
    assert job.status == JobStatus.FAILED
    assert job.status_code is None
    assert "Request error" in job.error
    assert "TimeoutException" in job.error


async def test_connection_error_marks_failed(store, monkeypatch):
    patch_async_client(
        monkeypatch, get_side_effect=httpx.ConnectError("connection refused")
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store)

    job = await store.get(job_id)
    assert job.status == JobStatus.FAILED
    assert "Request error" in job.error
    assert "ConnectError" in job.error


async def test_unexpected_error_marks_failed(store, monkeypatch):
    patch_async_client(monkeypatch, get_side_effect=RuntimeError("boom"))
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store)

    job = await store.get(job_id)
    assert job.status == JobStatus.FAILED
    assert "Unexpected error" in job.error
    assert "RuntimeError" in job.error


async def test_unknown_job_id_is_a_noop(store, monkeypatch):
    """If the job vanished, the worker logs and returns without raising."""
    patch_async_client(monkeypatch, response=make_response(200, json_data={}))

    # Should not raise even though this id was never added.
    await jobs.process_job(uuid.uuid4(), store)

    assert await store.list() == []
