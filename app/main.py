"""FastAPI application exposing the job processing REST API."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, status

from .models import Job, JobCreatedResponse, JobRequest
from .queue import QueueFull, job_queue
from .store import store

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the queue's worker pool on startup, drain it on shutdown."""
    await job_queue.start()
    try:
        yield
    finally:
        await job_queue.stop()


app = FastAPI(
    title="JobProcessor",
    description="Accept jobs and process them asynchronously in the background.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["meta"])
async def health() -> dict[str, object]:
    """Liveness probe, also exposes current queue depth for observability."""
    return {"status": "ok", "queue_depth": job_queue.depth()}


@app.post(
    "/jobs",
    response_model=JobCreatedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["jobs"],
)
async def create_job(payload: JobRequest) -> JobCreatedResponse:
    """Accept a job, put it on the queue, and return its id immediately.

    Pydantic validates `payload` (including target_url) before this body runs;
    invalid input is rejected automatically with a 422 response.
    """
    try:
        # Create the server-side record. status defaults to Queued and the
        # uuid is generated server-side by the Job model.
        job = Job(target_url=str(payload.target_url))
        await store.add(job)

        try:
            # Hand off to the queue's worker pool. Swapping AsyncioJobQueue for
            # a Redis/arq-backed JobQueue later requires no change here.
            await job_queue.enqueue(job.id)
        except QueueFull as exc:
            # Backpressure: don't leave an orphan Queued job that never runs.
            await store.remove(job.id)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Job queue is full; please retry later.",
            ) from exc

        return JobCreatedResponse(id=job.id, status=job.status)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        # Never leak internals; log the detail and return a clean 500.
        logging.exception("Failed to create job")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not create job.",
        ) from exc


@app.get("/job/{job_id}", response_model=Job, tags=["jobs"])
async def get_job(job_id: UUID) -> Job:
    """Return the current status and details of a single job."""
    try:
        job = await store.get(job_id)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to read job %s", job_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not read job.",
        ) from exc

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found.",
        )
    return job


@app.get("/getJobs", response_model=list[Job], tags=["jobs"])
async def get_jobs() -> list[Job]:
    """Return all jobs and their info."""
    try:
        return await store.list()
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to list jobs")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not list jobs.",
        ) from exc
