# JobProcessor

A small REST API built with **FastAPI** that accepts jobs and processes them
**asynchronously in the background**. When a job is submitted, the API validates
the input, generates a server-side job id, queues the work, and returns
immediately. The actual work (calling an external API) runs in the background,
and clients poll for the result.

## How it works

```
POST /jobs ─► validate ─► create Job (Queued) ─► enqueue ─► return id (202)
                                                    │
                       bounded asyncio.Queue  ◄─────┘
                                │
             ┌──────────────────┼──────────────────┐   (pool of N workers)
             ▼                  ▼                  ▼
        process_job        process_job        process_job
          ├─ status = Running
          ├─ httpx GET target_url
          ├─ raise_for_status()
          └─ status = Completed / Failed
```

Jobs are handed to an in-process **queue** with a fixed pool of worker
coroutines, rather than `BackgroundTasks`. This gives:

- **Backpressure** — the queue is bounded (`JOB_QUEUE_SIZE`); when full, `POST
  /jobs` returns `503` instead of accepting unbounded work.
- **A concurrency cap** — at most `JOB_MAX_WORKERS` jobs (and outbound HTTP
  calls) run at once.
- **Decoupling** — processing is independent of the request lifecycle.

### Configuration (environment variables)

| Variable          | Default | Meaning                              |
| ----------------- | ------- | ------------------------------------ |
| `JOB_MAX_WORKERS` | `4`     | Concurrent workers / max in-flight jobs |
| `JOB_QUEUE_SIZE`  | `1000`  | Max queued jobs before backpressure  |

### Scaling note (Phase 2)

The queue is **in-process and non-durable**: jobs are lost on restart, and it
does not span multiple processes. The `JobQueue` interface in `app/queue.py`
(and the `JobStore` interface) are the seams for a durable, horizontally
scalable upgrade — a `RedisJobQueue` (e.g. backed by **arq**) plus a Redis-backed
store, run as a separate worker process. Because the endpoints only depend on
those interfaces, that swap won't touch the API code. Run a **single process**
until then, since the in-memory store/queue aren't shared across workers.

## Endpoints

| Method | Path             | Description                                            |
| ------ | ---------------- | ------------------------------------------------------ |
| POST   | `/jobs`          | Submit a job. Returns `{id, status}` with `202`.       |
| GET    | `/job/{job_id}`  | Get one job's status and details. `404` if missing.    |
| GET    | `/getJobs`       | List all jobs and their info.                          |
| GET    | `/health`        | Liveness probe.                                        |

### Job statuses

`Queued` → `Running` → `Completed` **or** `Failed`

## Running locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

uvicorn app.main:app --reload
```

Interactive docs: http://127.0.0.1:8000/docs

## Example

```bash
# Submit a job
curl -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"target_url": "https://httpbin.org/get"}'
# -> {"id": "....", "status": "Queued"}

# Check its status
curl http://127.0.0.1:8000/job/<id>

# List all jobs
curl http://127.0.0.1:8000/getJobs
```

## Project layout

```
app/
  main.py     # FastAPI app + endpoints (validation & error handling), lifespan
  models.py   # JobStatus enum, JobRequest, Job, response models
  jobs.py     # process_job worker (httpx + raise_for_status)
  store.py    # concurrency-safe in-memory async job store (swap for Redis/DB later)
  queue.py    # JobQueue interface + in-process AsyncioJobQueue (worker pool)
  config.py   # env-driven settings (worker count, queue size)
tests/
  test_worker.py   # unit tests for process_job (mocked httpx)
  test_queue.py    # unit tests for the queue (backpressure, concurrency cap, drain)
  test_api.py      # integration tests for the endpoints (TestClient)
  helpers.py       # httpx AsyncMock/MagicMock helpers + polling helper
scripts/
  concurrency_test.py  # live load test asserting no torn reads under concurrency
```

## Testing

```bash
pip install -r requirements-dev.txt
pytest                 # runs unit + integration tests

# Optional: live concurrency stress test (needs a running server on :8300)
python scripts/concurrency_test.py http://127.0.0.1:8300
```

The unit and integration tests mock the external API (via `unittest.mock`),
so they run fast and require no network.

## Notes / future work

- Jobs are stored **in memory**, so they are lost on restart. The storage is
  isolated behind `JobStore`, so it can be swapped for Redis or a database
  without touching the endpoints.
- `BackgroundTasks` is used for now. For heavier or more durable workloads this
  can be replaced with a real queue (Celery / RQ / arq) by changing a single
  line in `create_job`.
