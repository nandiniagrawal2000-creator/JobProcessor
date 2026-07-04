"""Unit tests for the in-process AsyncioJobQueue.

These test the queue mechanics directly (backpressure, concurrency cap, graceful
drain) with a lightweight fake worker, plus one end-to-end check that wires the
real `process_job` through the queue with a mocked HTTP client.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from app import jobs
from app.models import Job, JobStatus
from app.queue import AsyncioJobQueue, QueueFull
from tests.helpers import make_response, patch_async_client


async def test_enqueue_is_processed_by_worker_pool():
    processed: list = []

    async def worker(job_id, store):
        processed.append(job_id)

    queue = AsyncioJobQueue(store=None, worker=worker, max_workers=2, max_queue_size=10)
    await queue.start()
    ids = [uuid4() for _ in range(5)]
    for job_id in ids:
        await queue.enqueue(job_id)
    await queue._queue.join()
    await queue.stop()

    assert set(processed) == set(ids)


async def test_backpressure_raises_queue_full():
    async def never(job_id, store):  # pragma: no cover - never runs
        await asyncio.sleep(999)

    # 0 workers => nothing drains => the bounded queue fills deterministically.
    queue = AsyncioJobQueue(store=None, worker=never, max_workers=0, max_queue_size=3)
    await queue.start()
    try:
        await queue.enqueue(uuid4())
        await queue.enqueue(uuid4())
        await queue.enqueue(uuid4())
        assert queue.depth() == 3
        with pytest.raises(QueueFull):
            await queue.enqueue(uuid4())
    finally:
        # No workers exist to drain, so drain manually before stop() (whose
        # join() would otherwise block waiting for task_done forever).
        while not queue._queue.empty():
            queue._queue.get_nowait()
            queue._queue.task_done()
        await queue.stop()


async def test_respects_max_workers_concurrency_cap():
    max_workers = 3
    current = 0
    peak = 0
    lock = asyncio.Lock()

    async def worker(job_id, store):
        nonlocal current, peak
        async with lock:
            current += 1
            peak = max(peak, current)
        await asyncio.sleep(0.03)
        async with lock:
            current -= 1

    queue = AsyncioJobQueue(
        store=None, worker=worker, max_workers=max_workers, max_queue_size=100
    )
    await queue.start()
    for _ in range(20):
        await queue.enqueue(uuid4())
    await queue._queue.join()
    await queue.stop()

    # Never exceeded the cap, and with 20 jobs it should have reached it.
    assert peak == max_workers


async def test_stop_drains_pending_jobs():
    processed: list = []

    async def worker(job_id, store):
        await asyncio.sleep(0.01)
        processed.append(job_id)

    queue = AsyncioJobQueue(store=None, worker=worker, max_workers=2, max_queue_size=100)
    await queue.start()
    ids = [uuid4() for _ in range(10)]
    for job_id in ids:
        await queue.enqueue(job_id)

    # stop() must let every already-queued job finish before returning.
    await queue.stop()
    assert len(processed) == 10


async def test_worker_error_does_not_kill_the_pool():
    processed: list = []

    async def worker(job_id, store):
        if len(processed) == 0:
            processed.append("boom")
            raise RuntimeError("first job explodes")
        processed.append(job_id)

    queue = AsyncioJobQueue(store=None, worker=worker, max_workers=1, max_queue_size=10)
    await queue.start()
    await queue.enqueue(uuid4())  # this one raises
    good = uuid4()
    await queue.enqueue(good)  # this one must still be processed
    await queue._queue.join()
    await queue.stop()

    assert good in processed


async def test_enqueue_before_start_raises():
    queue = AsyncioJobQueue(store=None, worker=None, max_workers=1, max_queue_size=1)
    with pytest.raises(RuntimeError):
        await queue.enqueue(uuid4())


async def test_end_to_end_real_worker_completes_job(store, monkeypatch):
    patch_async_client(monkeypatch, response=make_response(200, json_data={"ok": 1}))
    queue = AsyncioJobQueue(
        store=store, worker=jobs.process_job, max_workers=2, max_queue_size=10
    )
    await queue.start()

    job = Job(target_url="http://external.test/api")
    await store.add(job)
    await queue.enqueue(job.id)
    await queue._queue.join()
    await queue.stop()

    snapshot = await store.get(job.id)
    assert snapshot.status == JobStatus.COMPLETED
    assert snapshot.result == {"ok": 1}
