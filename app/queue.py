"""Job queue abstraction + an in-process asyncio implementation.

Why this exists
---------------
`BackgroundTasks` schedules a coroutine per request with no bound on how many
run at once and no decoupling from the request lifecycle. This module replaces
that with a real (if in-process) queue:

  * a *bounded* `asyncio.Queue` gives backpressure -- when it's full, enqueue
    fails fast so the API can return 503 instead of piling on unbounded work;
  * a fixed pool of worker coroutines gives a hard concurrency cap (at most
    `max_workers` jobs -- and outbound HTTP calls -- run simultaneously);
  * processing is decoupled from the request: POST returns immediately, workers
    drain the queue in the background.

The `JobQueue` ABC is the seam for Phase 2: a `RedisJobQueue` / arq-backed
implementation can be dropped in without touching the endpoints. It is still
single-process and non-durable (jobs are lost on restart) -- that's what the
Redis phase would fix.
"""

from __future__ import annotations

import abc
import asyncio
import logging
from typing import Awaitable, Callable, Optional
from uuid import UUID

from .config import settings
from .jobs import process_job
from .store import JobStore, store

logger = logging.getLogger("jobprocessor.queue")

# A worker callable: given a job id and the store, process the job.
Worker = Callable[[UUID, JobStore], Awaitable[None]]


class QueueFull(Exception):
    """Raised by enqueue() when the bounded queue has no free capacity."""


class JobQueue(abc.ABC):
    """Interface every queue implementation must satisfy."""

    @abc.abstractmethod
    async def start(self) -> None:
        """Spin up workers / connect to the broker."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Drain in-flight work and shut workers down gracefully."""

    @abc.abstractmethod
    async def enqueue(self, job_id: UUID) -> None:
        """Submit a job id for processing. Raises QueueFull under backpressure."""

    @abc.abstractmethod
    def depth(self) -> int:
        """Number of jobs currently waiting (best-effort, for observability)."""


class AsyncioJobQueue(JobQueue):
    """In-process queue backed by asyncio.Queue and a pool of worker tasks."""

    def __init__(
        self,
        store: JobStore,
        worker: Worker,
        *,
        max_workers: int,
        max_queue_size: int,
    ) -> None:
        self._store = store
        self._worker = worker
        self._max_workers = max_workers
        self._max_queue_size = max_queue_size
        # Created in start() so the queue binds to the running event loop
        # (important when the app is restarted on a fresh loop, e.g. in tests).
        self._queue: Optional[asyncio.Queue[UUID]] = None
        self._workers: list[asyncio.Task] = []
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._queue = asyncio.Queue(maxsize=self._max_queue_size)
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker_loop(i), name=f"job-worker-{i}")
            for i in range(self._max_workers)
        ]
        logger.info(
            "Job queue started: workers=%d max_queue=%d",
            self._max_workers,
            self._max_queue_size,
        )

    async def enqueue(self, job_id: UUID) -> None:
        if not self._running or self._queue is None:
            raise RuntimeError("Queue is not running")
        try:
            self._queue.put_nowait(job_id)
        except asyncio.QueueFull as exc:
            raise QueueFull("Job queue is at capacity") from exc

    def depth(self) -> int:
        return self._queue.qsize() if self._queue is not None else 0

    async def _worker_loop(self, index: int) -> None:
        assert self._queue is not None
        while True:
            job_id = await self._queue.get()
            try:
                # The worker (process_job) already captures all errors on the
                # job record; this guard is a final safety net so one bad job
                # can never kill the worker loop.
                await self._worker(job_id, self._store)
            except Exception:  # noqa: BLE001
                logger.exception("Worker %d crashed handling job %s", index, job_id)
            finally:
                self._queue.task_done()

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        # Graceful: let already-queued jobs finish before tearing down.
        if self._queue is not None:
            await self._queue.join()
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []
        logger.info("Job queue stopped")


# Single shared instance used by the app. Swap this construction for a
# RedisJobQueue in Phase 2 -- the endpoints only depend on the JobQueue API.
job_queue: JobQueue = AsyncioJobQueue(
    store=store,
    worker=process_job,
    max_workers=settings.max_workers,
    max_queue_size=settings.queue_size,
)
