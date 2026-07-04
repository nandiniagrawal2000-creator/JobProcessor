"""Pydantic models and enums that describe jobs and their lifecycle."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import AnyHttpUrl, BaseModel, Field


def _utcnow() -> datetime:
    """Timezone-aware UTC timestamp used for created_at / updated_at."""
    return datetime.now(timezone.utc)


class JobStatus(str, Enum):
    """The lifecycle states a job can move through."""

    QUEUED = "Queued"
    RUNNING = "Running"
    RETRYING = "Retrying"
    COMPLETED = "Completed"
    FAILED = "Failed"


class JobRequest(BaseModel):
    """Incoming payload for POST /jobs.

    `target_url` is validated as a real HTTP(S) URL by Pydantic, so malformed
    input is rejected with a 422 before we ever create a job.
    """

    target_url: AnyHttpUrl = Field(
        ...,
        description="External API endpoint the background worker will call.",
    )


class Job(BaseModel):
    """Server-side representation of a job and its current state."""

    id: UUID = Field(default_factory=uuid4)
    target_url: str
    status: JobStatus = JobStatus.QUEUED
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    # Number of processing attempts made so far (includes retries).
    attempts: int = 0

    # Populated once the background task finishes. These mirror whatever the
    # external API returned so the client sees the full response content.
    status_code: Optional[int] = None
    response_headers: Optional[dict[str, str]] = None
    result: Optional[Any] = None
    error: Optional[str] = None

    def touch(self, status: JobStatus) -> None:
        """Transition to a new status and bump the updated_at timestamp."""
        self.status = status
        self.updated_at = _utcnow()


class JobCreatedResponse(BaseModel):
    """Response returned immediately after a job is accepted."""

    id: UUID
    status: JobStatus
