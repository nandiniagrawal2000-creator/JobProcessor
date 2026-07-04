"""Background job processing logic."""

from __future__ import annotations

import logging
from uuid import UUID

import httpx

from .models import Job, JobStatus
from .store import JobStore

logger = logging.getLogger("jobprocessor.jobs")

# How long we wait on the external API before giving up.
REQUEST_TIMEOUT_SECONDS = 30.0


async def process_job(job_id: UUID, store: JobStore) -> None:
    """Run a single job in the background.

    Flow:
      1. Atomically mark the job Running.
      2. Call the external target_url with httpx (async) -- OUTSIDE any lock, so
         a slow request never blocks readers (GET /job, GET /getJobs).
      3. raise_for_status() turns any 4xx/5xx into an exception we catch.
      4. Atomically write the outcome (Completed / Failed).

    Every write to the shared job goes through `store.update`, which applies the
    change under the store lock in one synchronous shot. Readers therefore only
    ever see a fully-consistent snapshot -- either before or after a transition,
    never mid-transition.

    This function never raises: a background task has no client to return an
    error to, so every failure is captured on the job record instead.
    """
    running = await store.update(job_id, lambda j: j.touch(JobStatus.RUNNING))
    if running is None:
        # Should not happen (we add before scheduling), but guard anyway.
        logger.error("process_job called for unknown job_id=%s", job_id)
        return

    logger.info("Job %s running -> %s", running.id, running.target_url)

    try:
        # Network I/O happens with NO lock held.
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.get(running.target_url)
            # Raises httpx.HTTPStatusError for 4xx / 5xx responses.
            response.raise_for_status()

        status_code = response.status_code
        headers = dict(response.headers)
        body = _extract_body(response)

        def apply_success(job: Job) -> None:
            job.status_code = status_code
            job.response_headers = headers
            job.result = body
            job.error = None
            job.touch(JobStatus.COMPLETED)

        await store.update(job_id, apply_success)
        logger.info("Job %s completed (%s)", job_id, status_code)

    except httpx.HTTPStatusError as exc:
        # The server responded, but with an error status.
        code = exc.response.status_code
        message = f"External API returned {code}"

        def apply_http_error(job: Job) -> None:
            job.status_code = code
            job.error = message
            job.touch(JobStatus.FAILED)

        await store.update(job_id, apply_http_error)
        logger.warning("Job %s failed: %s", job_id, message)

    except httpx.RequestError as exc:
        # Network-level problem: DNS, connection refused, timeout, etc.
        message = f"Request error: {exc.__class__.__name__}: {exc}"

        def apply_request_error(job: Job) -> None:
            job.error = message
            job.touch(JobStatus.FAILED)

        await store.update(job_id, apply_request_error)
        logger.warning("Job %s failed: %s", job_id, message)

    except Exception as exc:  # noqa: BLE001 - last-resort safety net
        message = f"Unexpected error: {exc.__class__.__name__}: {exc}"

        def apply_unexpected(job: Job) -> None:
            job.error = message
            job.touch(JobStatus.FAILED)

        await store.update(job_id, apply_unexpected)
        logger.exception("Job %s crashed unexpectedly", job_id)


def _extract_body(response: httpx.Response):
    """Return JSON when possible, otherwise fall back to text."""
    try:
        return response.json()
    except ValueError:
        return response.text
