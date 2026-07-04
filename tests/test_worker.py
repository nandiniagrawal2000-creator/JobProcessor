"""Unit tests for the background worker `process_job`.

These test the worker in isolation (no HTTP server): we call `process_job`
directly against a fresh JobStore and a mocked httpx client, then inspect the
stored job to assert status transitions, retry behavior, and captured data.

The autouse `instant_sleep` fixture (see conftest) stubs the backoff wait, so
retries happen instantly and we can assert how often the worker waited.
"""

from __future__ import annotations

import uuid

import httpx
import pytest

from app import jobs
from app.models import Job, JobStatus
from tests.helpers import http_status_error, make_response, patch_async_client

# No-retry and fast-retry policies keep tests deterministic and independent of
# the environment-driven defaults.
NO_RETRY = jobs.RetryPolicy(
    max_retries=0, base_delay=0.0, max_delay=0.0, backoff_factor=2.0, jitter=False
)


def fast_policy(max_retries: int = 2) -> jobs.RetryPolicy:
    return jobs.RetryPolicy(
        max_retries=max_retries,
        base_delay=0.01,
        max_delay=0.02,
        backoff_factor=2.0,
        jitter=False,
    )


async def _queue_job(store, url: str = "http://external.test/api") -> uuid.UUID:
    """Create a Queued job in the store and return its id."""
    job = Job(target_url=url)
    await store.add(job)
    return job.id


# --------------------------------------------------------------------------- #
# Success paths
# --------------------------------------------------------------------------- #
async def test_success_marks_completed_with_json_result(store, monkeypatch, instant_sleep):
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
    assert job.attempts == 1
    assert instant_sleep.call_count == 0  # no retries on success
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


# --------------------------------------------------------------------------- #
# Permanent failures: NOT retried
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("code", [400, 401, 404, 422])
async def test_client_error_is_permanent_no_retry(store, monkeypatch, instant_sleep, code):
    client_mock = patch_async_client(
        monkeypatch,
        response=make_response(200, json_data={}, raise_for_status_exc=http_status_error(code)),
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store, fast_policy(3))

    job = await store.get(job_id)
    assert job.status == JobStatus.FAILED
    assert job.status_code == code
    assert job.attempts == 1
    assert client_mock.get.call_count == 1
    assert instant_sleep.call_count == 0


async def test_unexpected_error_is_permanent_no_retry(store, monkeypatch, instant_sleep):
    patch_async_client(monkeypatch, get_side_effect=RuntimeError("boom"))
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store, fast_policy(3))

    job = await store.get(job_id)
    assert job.status == JobStatus.FAILED
    assert "Unexpected error" in job.error
    assert "RuntimeError" in job.error
    assert job.attempts == 1
    assert instant_sleep.call_count == 0


# --------------------------------------------------------------------------- #
# Transient failures: retried with backoff
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("code", [429, 500, 503])
async def test_server_error_is_transient_retried_then_failed(
    store, monkeypatch, instant_sleep, code
):
    client_mock = patch_async_client(
        monkeypatch,
        response=make_response(200, json_data={}, raise_for_status_exc=http_status_error(code)),
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store, fast_policy(2))

    job = await store.get(job_id)
    assert job.status == JobStatus.FAILED
    assert job.status_code == code
    assert job.attempts == 3  # 1 initial + 2 retries
    assert client_mock.get.call_count == 3
    assert instant_sleep.call_count == 2  # waited between each retry


async def test_timeout_is_transient_retried_then_failed(store, monkeypatch, instant_sleep):
    client_mock = patch_async_client(
        monkeypatch, get_side_effect=httpx.TimeoutException("request timed out")
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store, fast_policy(2))

    job = await store.get(job_id)
    assert job.status == JobStatus.FAILED
    assert job.status_code is None
    assert "TimeoutException" in job.error
    assert job.attempts == 3
    assert client_mock.get.call_count == 3
    assert instant_sleep.call_count == 2


async def test_transient_error_then_success(store, monkeypatch, instant_sleep):
    """First attempt hits a connection error, retry succeeds -> Completed."""
    ok = make_response(200, json_data={"recovered": True})
    patch_async_client(
        monkeypatch,
        get_side_effect=[httpx.ConnectError("refused"), ok],
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store, fast_policy(3))

    job = await store.get(job_id)
    assert job.status == JobStatus.COMPLETED
    assert job.result == {"recovered": True}
    assert job.error is None
    assert job.attempts == 2
    assert instant_sleep.call_count == 1


async def test_marks_retrying_during_backoff(store, monkeypatch, instant_sleep):
    """The job is in the Retrying state while it waits between attempts."""
    seen_statuses = []

    async def record(_delay):
        snapshot = await store.get(job_id)
        seen_statuses.append(snapshot.status)

    instant_sleep.side_effect = record
    patch_async_client(
        monkeypatch,
        get_side_effect=[httpx.ConnectError("refused"), make_response(200, json_data={})],
    )
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store, fast_policy(3))

    assert seen_statuses == [JobStatus.RETRYING]
    assert (await store.get(job_id)).status == JobStatus.COMPLETED


async def test_simulated_work_runs_before_external_call(store, monkeypatch, no_simulated_work):
    """Each attempt performs the simulated work step before calling out."""
    from app.config import settings

    patch_async_client(monkeypatch, response=make_response(200, json_data={}))
    job_id = await _queue_job(store)

    await jobs.process_job(job_id, store)

    no_simulated_work.assert_awaited_once_with(settings.simulated_work_seconds)


async def test_unknown_job_id_is_a_noop(store, monkeypatch):
    """If the job vanished, the worker logs and returns without raising."""
    patch_async_client(monkeypatch, response=make_response(200, json_data={}))

    await jobs.process_job(uuid.uuid4(), store)

    assert await store.list() == []


# --------------------------------------------------------------------------- #
# Backoff math (pure function)
# --------------------------------------------------------------------------- #
def test_delay_for_is_exponential_and_capped_without_jitter():
    policy = jobs.RetryPolicy(
        max_retries=5, base_delay=1.0, max_delay=100.0, backoff_factor=2.0, jitter=False
    )
    assert policy.delay_for(0) == 1.0
    assert policy.delay_for(1) == 2.0
    assert policy.delay_for(2) == 4.0
    assert policy.delay_for(3) == 8.0
    assert policy.delay_for(20) == 100.0  # capped at max_delay


def test_delay_for_jitter_stays_within_bounds():
    policy = jobs.RetryPolicy(
        max_retries=5, base_delay=1.0, max_delay=100.0, backoff_factor=2.0, jitter=True
    )
    for attempt in range(5):
        upper = min(100.0, 1.0 * 2.0 ** attempt)
        for _ in range(20):
            assert 0.0 <= policy.delay_for(attempt) <= upper
