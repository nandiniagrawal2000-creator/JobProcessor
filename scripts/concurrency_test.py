"""Concurrency stress test.

Submits many jobs while simultaneously hammering GET /job/{id} and GET /getJobs.
It asserts that readers never see a torn/inconsistent job, e.g.:
  * status == Completed  => result present and error is None
  * status == Failed     => error present
and that /getJobs never raises (no "dict changed size during iteration").
"""

from __future__ import annotations

import asyncio
import sys

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8300"
NUM_JOBS = 40


def check_consistency(job: dict) -> None:
    status = job["status"]
    if status == "Completed":
        assert job["result"] is not None, f"Completed but no result: {job}"
        assert job["error"] is None, f"Completed but has error: {job}"
        assert job["status_code"] is not None, f"Completed but no status_code: {job}"
    elif status == "Failed":
        assert job["error"] is not None, f"Failed but no error: {job}"


async def submit_jobs(client: httpx.AsyncClient) -> list[str]:
    ids = []
    for i in range(NUM_JOBS):
        target = f"{BASE}/health" if i % 3 else "http://127.0.0.1:9/down"
        r = await client.post("/jobs", json={"target_url": target})
        assert r.status_code == 202, r.text
        ids.append(r.json()["id"])
    return ids


async def hammer_reads(client: httpx.AsyncClient, ids: list[str], stop: asyncio.Event) -> int:
    reads = 0
    while not stop.is_set():
        # Hammer /getJobs (list mutation risk) ...
        r = await client.get("/getJobs")
        assert r.status_code == 200
        for job in r.json():
            check_consistency(job)
        # ... and individual reads (torn-read risk).
        for jid in ids:
            rr = await client.get(f"/job/{jid}")
            if rr.status_code == 200:
                check_consistency(rr.json())
        reads += 1
    return reads


async def main() -> None:
    async with httpx.AsyncClient(base_url=BASE, timeout=10) as client:
        stop = asyncio.Event()
        # Start readers first, then submit jobs concurrently.
        readers = [asyncio.create_task(hammer_reads(client, [], stop)) for _ in range(5)]
        ids = await submit_jobs(client)
        # Point readers at the real ids by racing more reader rounds.
        reader2 = [asyncio.create_task(hammer_reads(client, ids, stop)) for _ in range(8)]
        await asyncio.sleep(3)
        stop.set()
        await asyncio.gather(*readers, *reader2)

        final = (await client.get("/getJobs")).json()
        done = [j for j in final if j["status"] in ("Completed", "Failed")]
        print(f"submitted={len(ids)} final_jobs={len(final)} settled={len(done)}")
        for j in final:
            check_consistency(j)
        print("ALL CONSISTENCY CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
