"""Background job processing logic with retries + exponential backoff."""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Optional
from uuid import UUID

import httpx

from .config import settings
from .models import Job, JobStatus
from .store import JobStore

logger = logging.getLogger("jobprocessor.jobs")


@dataclass(frozen=True)
class RetryPolicy:
    """Controls how transient failures are retried."""

    max_retries: int
    base_delay: float
    max_delay: float
    backoff_factor: float
    jitter: bool = True

    def delay_for(self, attempt: int) -> float:
        """Backoff delay before the retry that follows `attempt` (0-indexed).

        Exponential: base * factor**attempt, capped at max_delay. With jitter we
        pick a random value in [0, delay] ("full jitter") to avoid thundering
        herds when many jobs fail at once.
        """
        raw = self.base_delay * (self.backoff_factor ** attempt)
        delay = min(self.max_delay, raw)
        if self.jitter:
            delay = random.uniform(0, delay)
        return delay


DEFAULT_RETRY_POLICY = RetryPolicy(
    max_retries=settings.max_retries,
    base_delay=settings.retry_base_delay,
    max_delay=settings.retry_max_delay,
    backoff_factor=settings.retry_backoff_factor,
)


async def _sleep(seconds: float) -> None:
    """Indirection around asyncio.sleep so tests can stub the backoff wait."""
    await asyncio.sleep(seconds)


async def _simulate_work(seconds: float) -> None:
    """Simulate extra processing time before the external API call.

    Stands in for real CPU/IO work a job might do so runs take more than a few
    milliseconds. Configurable via JOB_SIMULATED_WORK_SECONDS (0 disables).
    Kept as its own coroutine so tests can stub it out.
    """
    if seconds > 0:
        await asyncio.sleep(seconds)


def _classify(exc: Exception) -> tuple[bool, Optional[int], str]:
    """Map an exception to (is_transient, status_code, message).

    Transient (worth retrying):
      * httpx.RequestError  -> timeouts, connection errors, DNS, etc.
      * HTTP 429 or any 5xx -> server is overloaded / temporarily unavailable.
    Permanent (do not retry):
      * HTTP 4xx (except 429) -> the request itself is bad; retrying won't help.
      * Any other exception   -> treat as a bug, not a transient blip.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        transient = code == 429 or code >= 500
        return transient, code, f"External API returned {code}"
    if isinstance(exc, httpx.RequestError):
        return True, None, f"Request error: {exc.__class__.__name__}: {exc}"
    return False, None, f"Unexpected error: {exc.__class__.__name__}: {exc}"


async def process_job(
    job_id: UUID,
    store: JobStore,
    retry_policy: Optional[RetryPolicy] = None,
) -> None:
    """Run a single job, retrying transient failures with exponential backoff.

    Flow per attempt: mark Running -> httpx GET -> raise_for_status(). On success
    the job is Completed. On a transient error we mark it Retrying, wait
    (backoff), and try again up to `max_retries` times; a permanent error (or
    exhausted retries) marks it Failed. Network I/O happens with no store lock
    held; every state change is applied atomically via store.update().

    This function never raises: failures are recorded on the job record.
    """
    policy = retry_policy or DEFAULT_RETRY_POLICY

    job = await store.get(job_id)
    if job is None:
        # Should not happen (we add before scheduling), but guard anyway.
        logger.error("process_job called for unknown job_id=%s", job_id)
        return
    target_url = job.target_url

    for attempt in range(policy.max_retries + 1):
        attempt_no = attempt + 1

        def mark_running(job: Job, n: int = attempt_no) -> None:
            job.attempts = n
            job.touch(JobStatus.RUNNING)

        await store.update(job_id, mark_running)
        logger.info("Job %s attempt %d -> %s", job_id, attempt_no, target_url)

        try:
            # Simulate the job doing some work, then make the external call.
            await _simulate_work(settings.simulated_work_seconds)
            async with httpx.AsyncClient(timeout=settings.request_timeout_seconds) as client:
                response = await client.get(target_url)
                # Raises httpx.HTTPStatusError for 4xx / 5xx responses.
                response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            transient, status_code, message = _classify(exc)
            has_retries_left = attempt < policy.max_retries

            if transient and has_retries_left:
                delay = policy.delay_for(attempt)

                def mark_retrying(
                    job: Job, msg: str = message, code=status_code
                ) -> None:
                    job.status_code = code
                    job.error = msg
                    job.touch(JobStatus.RETRYING)

                await store.update(job_id, mark_retrying)
                logger.warning(
                    "Job %s attempt %d failed (%s); retrying in %.2fs",
                    job_id,
                    attempt_no,
                    message,
                    delay,
                )
                await _sleep(delay)
                continue

            # Permanent error, or transient but out of retries.
            def mark_failed(job: Job, msg: str = message, code=status_code) -> None:
                job.status_code = code
                job.error = msg
                job.touch(JobStatus.FAILED)

            await store.update(job_id, mark_failed)
            logger.warning(
                "Job %s failed after %d attempt(s): %s", job_id, attempt_no, message
            )
            return

        # Success.
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
        logger.info("Job %s completed (%s) on attempt %d", job_id, status_code, attempt_no)
        return


def _extract_body(response: httpx.Response):
    """Return JSON when possible, otherwise fall back to text."""
    try:
        return response.json()
    except ValueError:
        return response.text
