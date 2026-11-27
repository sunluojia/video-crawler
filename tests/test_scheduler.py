import asyncio
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest

from video_crawler.errors import Wait
from video_crawler.scheduler import CRAWL, DOWNLOAD, ClaimedTask, Scheduler
from video_crawler.settings import Settings
from video_crawler.store import IdentityStore


@pytest.mark.parametrize("failure", [None, httpx.WriteError, asyncio.CancelledError])
async def test_upload_closes_file_when_transport_stops_reading(tmp_path, monkeypatch, failure):
    path = tmp_path / "video.mp4"
    first_chunk = b"x" * (1 << 20)
    path.write_bytes(first_chunk + b"tail")
    opened = []
    original_open = Path.open

    def track_open(file_path, mode="r", *args, **kwargs):
        file = original_open(file_path, mode, *args, **kwargs)
        if file_path == path and mode == "rb":
            opened.append(file)
        return file

    monkeypatch.setattr(Path, "open", track_open)

    class PartialUpload(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            # The transport can retain the request body without consuming the remaining chunks.
            self.body = aiter(request.stream)
            assert await anext(self.body) == first_chunk
            if failure is not None:
                raise failure("upload interrupted")
            return httpx.Response(409)

    settings = Settings(instance_id="crawler-test", service_token="t" * 16)
    scheduler = Scheduler(settings)
    await scheduler._http.aclose()
    task = ClaimedTask(str(uuid4()), 1, "download_video", str(uuid4()), {})
    digest = hashlib.sha256(first_chunk + b"tail").hexdigest()
    async with httpx.AsyncClient(
        base_url=settings.scheduler_url, transport=PartialUpload()
    ) as client:
        scheduler._http = client
        if failure is None:
            assert await scheduler.upload(task, path, digest) == 409
        else:
            with pytest.raises(failure):
                await scheduler.upload(task, path, digest)
        assert len(opened) == 1
        assert opened[0].closed
        path.unlink()


async def test_scheduler_claim_defer_leases_heartbeat_and_upload_share_executor_identity(tmp_path):
    task_id, attempt_id, batch_id, lease_id = (str(uuid4()) for _ in range(4))
    settings = Settings(instance_id="crawler-test", service_token="t" * 16)
    scheduler = Scheduler(settings)
    ownership = {
        "executor_id": settings.instance_id,
        "executor_session_id": str(scheduler.session_id),
    }
    wire_task = {
        "task_id": task_id,
        "kind": "download_video",
        "run": 2,
        "attempt_id": attempt_id,
        "attempt_no": 4,
        "lease_expires_at": "2026-11-25T01:00:00Z",
        "attempts": 1,
        "avoid": ["old-hk"],
        "history": ["第 1 次：下载太慢"],
        "body": {
            "task_id": task_id,
            "batch_id": batch_id,
            "run": 2,
            "creator": {"handle": "example", "oecuid": None},
            "work": {"work_id": "7", "url": "https://www.tiktok.com/@example/video/7"},
        },
    }
    received = []

    async def route(request):
        received.append(request)
        assert request.headers["Authorization"] == "Bearer " + "t" * 16
        match request.url.path:
            case "/api/crawler-tasks/claim":
                return httpx.Response(200, json={"tasks": [wire_task]})
            case "/api/proxy-leases":
                return httpx.Response(
                    200,
                    json={
                        "lease": {
                            "lease_id": lease_id,
                            "line": "hk",
                            "route": {"kind": "direct"},
                        }
                    },
                )
            case "/api/crawler-services/crawler-test" if request.method == "PUT":
                return httpx.Response(200, json={"revoked_attempts": [attempt_id]})
            case _:
                return httpx.Response(204)

    await scheduler._http.aclose()
    scheduler._http = httpx.AsyncClient(
        base_url=settings.scheduler_url,
        headers={"Authorization": "Bearer " + "t" * 16},
        transport=httpx.MockTransport(route),
    )
    path = tmp_path / "video.mp4"
    path.write_bytes(b"MP4")
    digest = hashlib.sha256(b"MP4").hexdigest()
    try:
        [task] = await scheduler.claim({"download_video": 1})
        assert task == ClaimedTask(**wire_task) and task.batch_id == batch_id
        await scheduler.lease(CRAWL)
        await scheduler.lease(DOWNLOAD, task.task_id, task.avoid, attempt_id=task.attempt_id)
        reply = await scheduler.register("http://crawler", "test", {}, {task.attempt_id})
        assert reply["revoked_attempts"] == [attempt_id]
        assert await scheduler.upload(task, path, digest) == 204
        await scheduler.defer(
            task.attempt_id, 60.2, "下载太慢", consume_attempt=True, avoid_line="hk"
        )
    finally:
        await scheduler.withdraw()

    claim, warm, download, beat, upload, defer, _ = received
    assert claim.method == "POST"
    assert json.loads(claim.content) == ownership | {"kinds": {"download_video": 1}}
    assert json.loads(warm.content) == {
        "device": settings.instance_id,
        "use": CRAWL,
        "task_id": None,
        "avoid": [],
    }
    assert json.loads(download.content) == {
        "device": settings.instance_id,
        "use": DOWNLOAD,
        "task_id": task_id,
        "avoid": ["old-hk"],
        "attempt_id": attempt_id,
    }
    assert (beat.method, beat.url.path) == ("PUT", "/api/crawler-services/crawler-test")
    assert json.loads(beat.content) == {
        "url": "http://crawler",
        "version": "test",
        "leases": [lease_id],
        "identities": {},
        "executor_session_id": str(scheduler.session_id),
        "task_attempts": [attempt_id],
    }
    assert upload.url.path == f"/api/download-tasks/{task_id}/runs/2/video"
    assert upload.method == "PUT" and upload.content == b"MP4"
    assert upload.headers["X-Content-SHA256"] == digest
    assert upload.headers["X-Attempt-ID"] == attempt_id
    assert upload.headers["X-Executor-ID"] == settings.instance_id
    assert upload.headers["X-Executor-Session-ID"] == str(scheduler.session_id)
    assert defer.url.path == f"/api/crawler-tasks/attempts/{attempt_id}/defer"
    assert defer.method == "POST"
    assert json.loads(defer.content) == ownership | {
        "retry_after": 61,
        "reason": "下载太慢",
        "consume_attempt": True,
        "avoid_line": "hk",
    }


def test_identity_redis_keys_are_per_device_and_retirement_removes_active_record():
    client = MagicMock()
    pipe = client.pipeline.return_value.__enter__.return_value
    pipe.hvals.return_value = pipe.zrange.return_value = pipe
    retired = {"id": "old", "retired_at": 900}
    active = {"id": "current", "retired_at": None}
    pipe.execute.return_value = [[json.dumps(active)], [json.dumps(retired)]]
    store = IdentityStore(client, "crawler-1")
    store.save([retired, active])
    assert store.records() == [active, retired]

    key = "video-crawler:{crawler-1}:identities"
    pipe.hset.assert_called_once_with(key, mapping={"current": json.dumps(active)})
    pipe.hdel.assert_called_once_with(key, "old")
    pipe.zadd.assert_called_once_with(f"{key}:retired", {json.dumps(retired): 900})
    pipe.hvals.assert_called_once_with(key)
    pipe.zrange.assert_called_once_with(f"{key}:retired", 0, -1)


async def test_claim_with_wait_reads_optional_crawl_wait_hint():
    settings = Settings(service_token="t" * 16)
    scheduler = Scheduler(settings)
    response = MagicMock()
    response.raise_for_status.return_value = response
    response.json.return_value = {
        "tasks": [],
        "crawl_wait": {
            "retry_after": 181,
            "reason": "采集线路正在冷却",
            "reason_code": "crawl_empty_cooldown",
        },
    }
    scheduler._http.post = AsyncMock(return_value=response)

    tasks, wait = await scheduler.claim_with_wait({"crawl_creator": 2, "download_video": 4})
    assert tasks == []
    assert isinstance(wait, Wait)
    assert (wait.seconds, wait.use, wait.code) == (
        181,
        "crawler.crawl_creator",
        "crawl_empty_cooldown",
    )


async def test_claim_with_wait_accepts_old_reply_without_hint():
    settings = Settings(service_token="t" * 16)
    scheduler = Scheduler(settings)
    response = MagicMock()
    response.raise_for_status.return_value = response
    response.json.return_value = {"tasks": []}
    scheduler._http.post = AsyncMock(return_value=response)

    tasks, wait = await scheduler.claim_with_wait({"crawl_creator": 1})
    assert tasks == [] and wait is None


def test_protected_retirement_keeps_a_device_archive_outside_the_recent_window():
    client = MagicMock()
    pipe = client.pipeline.return_value.__enter__.return_value
    pipe.hvals.return_value = pipe.zrange.return_value = pipe
    key = "video-crawler:{crawler-1}:identities"
    archived = {"id": "archived", "retired_at": 900}
    recent = {"id": "recent", "retired_at": 960}
    store = IdentityStore(client, "crawler-1", keep_profile_dirs=True)
    store.save([archived])
    pipe.hset.assert_called_once_with(
        f"{key}:kept-retired",
        mapping={
            "archived": json.dumps(archived),
        },
    )
    pipe.execute.return_value = [[], [json.dumps(recent)], [json.dumps(archived)]]
    assert store.records() == [archived, recent]
    client.hset.assert_called_once_with(
        f"{key}:kept-retired",
        mapping={
            "recent": json.dumps(recent),
        },
    )
