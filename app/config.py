"""Runtime configuration, read from environment variables.

Kept dependency-free (plain os.getenv) on purpose. If config grows, this is the
natural place to switch to pydantic-settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Number of concurrent worker coroutines pulling from the queue. This is the
    # hard cap on how many jobs (and thus outbound HTTP calls) run at once.
    max_workers: int = int(os.getenv("JOB_MAX_WORKERS", "4"))

    # Max number of jobs allowed to wait in the queue. Enqueuing beyond this
    # applies backpressure: POST /jobs returns 503 instead of accepting more.
    queue_size: int = int(os.getenv("JOB_QUEUE_SIZE", "1000"))


settings = Settings()
