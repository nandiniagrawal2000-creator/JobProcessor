"""A concurrency-safe in-memory job store.

Concurrency model
------------------
FastAPI request handlers and BackgroundTasks all run on a single asyncio event
loop. That means Python only switches between coroutines at `await` points, so
there is no thread-style preemption *inside* a block of synchronous code.

The danger is therefore not "two writers at the exact same instant" but:

  1. A reader observing a job *reference* while a writer mutates that same
     object across several fields (a "torn read": e.g. status already flipped to
     Completed but `result` not yet attached).
  2. Iterating the jobs dict while another coroutine adds/removes an entry.

This store closes both gaps with two rules:

  * Every mutation happens inside a short critical section guarded by an
    `asyncio.Lock`, and the mutation is a *synchronous* callback -- it cannot
    `await`, so the whole change is applied atomically before any other
    coroutine can observe it.
  * Every read returns a deep copy (snapshot) taken while holding the lock, so
    callers get a consistent point-in-time view that is fully decoupled from the
    live object a background worker may still be mutating.

Long-running I/O (the external HTTP call) is deliberately kept *outside* the
lock by the caller, so reads never block behind a slow network request.

The dict/lock is an implementation detail hidden behind this interface, so it
can later be swapped for Redis / a database without touching the endpoints.
"""

from __future__ import annotations

import asyncio
from typing import Callable, Optional
from uuid import UUID

from .models import Job


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[UUID, Job] = {}
        self._lock = asyncio.Lock()

    async def add(self, job: Job) -> None:
        """Insert a job. Stores a copy so the caller's reference is decoupled."""
        async with self._lock:
            self._jobs[job.id] = job.model_copy(deep=True)

    async def get(self, job_id: UUID) -> Optional[Job]:
        """Return a consistent snapshot of a single job, or None if missing."""
        async with self._lock:
            job = self._jobs.get(job_id)
            return job.model_copy(deep=True) if job is not None else None

    async def list(self) -> list[Job]:
        """Return snapshots of all jobs.

        Both the list and each job are copied under the lock, so we neither
        iterate a mutating dict nor hand back references to live objects.
        """
        async with self._lock:
            return [job.model_copy(deep=True) for job in self._jobs.values()]

    async def remove(self, job_id: UUID) -> None:
        """Delete a job if present (used to clean up an accepted-but-not-queued
        job when the queue rejects it under backpressure)."""
        async with self._lock:
            self._jobs.pop(job_id, None)

    async def update(
        self, job_id: UUID, mutate: Callable[[Job], None]
    ) -> Optional[Job]:
        """Atomically apply `mutate` to the stored job and return a snapshot.

        `mutate` must be synchronous (no `await`). It runs while the lock is
        held, so the entire set of field changes it makes becomes visible to
        readers all at once -- never half-applied.
        """
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            mutate(job)
            return job.model_copy(deep=True)


# Single shared instance used by the app.
store = JobStore()
