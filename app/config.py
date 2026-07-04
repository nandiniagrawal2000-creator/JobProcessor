"""Application configuration via pydantic-settings.

Values are read (in priority order) from:
  1. environment variables, prefixed with ``JOB_`` (e.g. ``JOB_MAX_WORKERS``);
  2. a local ``.env`` file (not committed -- see ``.env.example``);
  3. the defaults declared below.

Types are validated/coerced by pydantic, and constraints (e.g. non-negative)
are enforced at startup, so a bad value fails fast with a clear error instead
of surfacing deep inside the worker.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="JOB_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Logging -------------------------------------------------------------
    # Log level for the app's own loggers. Env: JOB_LOG_LEVEL
    log_level: str = Field(default="INFO")

    # --- Queue / worker pool -------------------------------------------------
    # Concurrent worker coroutines pulling from the queue -- the hard cap on how
    # many jobs (and outbound HTTP calls) run at once.  Env: JOB_MAX_WORKERS
    max_workers: int = Field(default=4, ge=1)

    # Max jobs allowed to wait in the queue; beyond this POST /jobs returns 503.
    # Env: JOB_QUEUE_SIZE
    queue_size: int = Field(default=1000, ge=1)

    # --- External call -------------------------------------------------------
    # Per-attempt timeout (seconds) for the outbound httpx call.
    # Env: JOB_REQUEST_TIMEOUT_SECONDS
    request_timeout_seconds: float = Field(default=30.0, gt=0)

    # Simulated processing time (seconds) before the external call, to mimic a
    # longer-running job. Set to 0 to disable. Env: JOB_SIMULATED_WORK_SECONDS
    simulated_work_seconds: float = Field(default=2.0, ge=0)

    # --- Retry policy (transient failures only) ------------------------------
    # Total attempts = max_retries + 1.  Env: JOB_MAX_RETRIES
    max_retries: int = Field(default=3, ge=0)

    # Backoff = base_delay * (backoff_factor ** attempt), capped at max_delay,
    # then full-jittered. Envs: JOB_RETRY_BASE_DELAY / _MAX_DELAY / _BACKOFF_FACTOR
    retry_base_delay: float = Field(default=0.5, ge=0)
    retry_max_delay: float = Field(default=10.0, ge=0)
    retry_backoff_factor: float = Field(default=2.0, ge=1)


settings = Settings()
