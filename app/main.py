"""FastAPI application exposing the job processing REST API."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse

from .config import settings
from .models import Job, JobCreatedResponse, JobRequest
from .queue import QueueFull, job_queue
from .store import store

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("jobprocessor.api")


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
    redoc_url=None,
)


@app.get("/", response_class=HTMLResponse, include_in_schema=False, tags=["meta"])
async def index() -> str:
    """Human-friendly landing page confirming the service is up, with links."""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{app.title}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 640px; margin: 3rem auto;
            padding: 0 1rem; color: #1a1a1a; line-height: 1.6; }}
    h1 {{ margin-bottom: 0.25rem; }}
    .status {{ color: #0a7d28; font-weight: 600; }}
    ul {{ padding-left: 1.2rem; }}
    code {{ background: #f2f2f2; padding: 0.1rem 0.35rem; border-radius: 4px; }}
    a {{ color: #1a56db; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <h1>{app.title}</h1>
  <p class="status">&#9679; Service is running (v{app.version})</p>
  <p>An async job processing API. Explore it here:</p>
  <ul>
    <li><a href="/docs">/docs</a> &mdash; interactive Swagger API docs</li>
    <li><a href="/health">/health</a> &mdash; liveness probe + queue depth</li>
    <li><a href="/getJobs">/getJobs</a> &mdash; list all jobs</li>
  </ul>
  <p>Submit a job with <code>POST /jobs</code> (see <a href="/docs">/docs</a>).</p>
</body>
</html>"""


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
    logger.info("POST /jobs received (target_url=%s)", payload.target_url)
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
            logger.warning(
                "Rejected job for %s: queue full (depth=%d)",
                job.target_url,
                job_queue.depth(),
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Job queue is full; please retry later.",
            ) from exc

        logger.info(
            "Job %s accepted and queued (queue_depth=%d)", job.id, job_queue.depth()
        )
        return JobCreatedResponse(id=job.id, status=job.status)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        # Never leak internals; log the detail and return a clean 500.
        logger.exception("Failed to create job")
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
        logger.exception("Failed to read job %s", job_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not read job.",
        ) from exc

    if job is None:
        logger.info("GET /job/%s -> 404 not found", job_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found.",
        )
    logger.info("GET /job/%s -> %s (attempts=%d)", job_id, job.status.value, job.attempts)
    return job


@app.get("/getJobs", response_model=list[Job], tags=["jobs"])
async def get_jobs() -> list[Job]:
    """Return all jobs and their info."""
    try:
        jobs = await store.list()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to list jobs")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not list jobs.",
        ) from exc

    logger.info("GET /getJobs -> %d job(s)", len(jobs))
    return jobs
