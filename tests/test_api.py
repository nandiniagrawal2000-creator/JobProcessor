"""Integration tests for the HTTP API using FastAPI's TestClient.

These exercise the real endpoints end-to-end (routing, Pydantic validation,
status codes, background task wiring). The only thing mocked is the external
HTTP call the background worker makes, so no real network is used.

Because Starlette's TestClient runs a request's BackgroundTasks to completion
before returning, a POST /jobs has already been fully processed by the time we
read it back with GET /job/{id}.
"""

from __future__ import annotations

import uuid

import httpx

from tests.helpers import (
    http_status_error,
    make_response,
    patch_async_client,
    wait_for_terminal,
)


def test_health_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["queue_depth"] == 0


def test_create_job_happy_path(client, monkeypatch):
    patch_async_client(
        monkeypatch, response=make_response(200, json_data={"hello": "world"})
    )

    response = client.post("/jobs", json={"target_url": "https://api.example.com/data"})

    # Immediate acknowledgement (processing happens asynchronously).
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "Queued"
    job_id = body["id"]
    uuid.UUID(job_id)  # id is a valid uuid

    # A worker picks it up and completes it with the mocked payload.
    job = wait_for_terminal(client, job_id)
    assert job["status"] == "Completed"
    assert job["status_code"] == 200
    assert job["result"] == {"hello": "world"}
    assert job["error"] is None


def test_create_job_external_failure_marks_failed(client, monkeypatch):
    patch_async_client(monkeypatch, get_side_effect=httpx.ConnectError("refused"))

    response = client.post("/jobs", json={"target_url": "https://down.example.com"})
    assert response.status_code == 202
    job_id = response.json()["id"]

    job = wait_for_terminal(client, job_id)
    assert job["status"] == "Failed"
    assert "Request error" in job["error"]


def test_create_job_http_error_marks_failed(client, monkeypatch):
    patch_async_client(
        monkeypatch,
        response=make_response(200, json_data={}, raise_for_status_exc=http_status_error(503)),
    )

    response = client.post("/jobs", json={"target_url": "https://api.example.com"})
    job_id = response.json()["id"]

    job = wait_for_terminal(client, job_id)
    assert job["status"] == "Failed"
    assert job["status_code"] == 503


def test_create_job_invalid_url_returns_422(client):
    response = client.post("/jobs", json={"target_url": "not-a-url"})
    assert response.status_code == 422


def test_create_job_missing_field_returns_422(client):
    response = client.post("/jobs", json={})
    assert response.status_code == 422


def test_create_job_wrong_type_returns_422(client):
    response = client.post("/jobs", json={"target_url": 12345})
    assert response.status_code == 422


def test_get_unknown_job_returns_404(client):
    response = client.get(f"/job/{uuid.uuid4()}")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_get_job_with_malformed_uuid_returns_422(client):
    response = client.get("/job/not-a-uuid")
    assert response.status_code == 422


def test_get_jobs_empty_initially(client):
    response = client.get("/getJobs")
    assert response.status_code == 200
    assert response.json() == []


def test_get_jobs_lists_all_created(client, monkeypatch):
    patch_async_client(monkeypatch, response=make_response(200, json_data={"ok": True}))

    id_a = client.post("/jobs", json={"target_url": "https://a.example.com"}).json()["id"]
    id_b = client.post("/jobs", json={"target_url": "https://b.example.com"}).json()["id"]
    wait_for_terminal(client, id_a)
    wait_for_terminal(client, id_b)

    response = client.get("/getJobs")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    targets = {job["target_url"] for job in data}
    assert targets == {"https://a.example.com/", "https://b.example.com/"}
    assert all(job["status"] == "Completed" for job in data)
