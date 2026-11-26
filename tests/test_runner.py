"""从中心按需认领、归还重试、续租和撤销尝试。"""

import asyncio
import time
from dataclasses import replace
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call
from uuid import uuid4

import httpx
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from video_crawler import runner as runner_module
from video_crawler import tiktok
from video_crawler.api import control_api
from video_crawler.browser import Browsers, Session
from video_crawler.download import Downloader
from video_crawler.errors import Gone, Retry, Wait
from video_crawler.identities import Identity, IdentityPool
from video_crawler.main import heartbeat, publish_event
from video_crawler.runner import CRAWL_CREATOR, DOWNLOAD_VIDEO, Runner
from video_crawler.scheduler import CRAWL, DOWNLOAD, ClaimedTask, Lease
from video_crawler.settings import Settings

SLOTS = {CRAWL_CREATOR: 2, DOWNLOAD_VIDEO: 4}
TOKEN = "t" * 16


def task(kind=DOWNLOAD_VIDEO, task_id="t1", batch_id="b1", **fields) -> ClaimedTask:
    return ClaimedTask(
        task_id,
        1,
        kind,
        str(uuid4()),
        {
            "batch_id": batch_id,
            "work": {"work_id": "7", "url": "https://www.tiktok.com/@a/video/7"},
        },
        **fields,
    )


def server(*tasks) -> SimpleNamespace:
    pending = list(tasks)

    async def claim(kinds):
        result = []
        for kind, count in kinds.items():
            for found in [entry for entry in pending if entry.kind == kind][:count]:
                pending.remove(found)
                result.append(found)
        return result

    return SimpleNamespace(
        instance="crawler-1",
        session_id=uuid4(),
        claim=AsyncMock(side_effect=claim),
        defer=AsyncMock(),
        leases=set(),
    )


def run_with(
    scheduler, handler, *, slots=SLOTS, available=lambda: 1, publish=None, cached_download=None
) -> Runner:
    return Runner(
        scheduler,
        {CRAWL_CREATOR: handler, DOWNLOAD_VIDEO: handler},
        slots,
        available_identities=available,
        publish=publish or AsyncMock(),
        cached_download=cached_download,
    )


async def cancel_jobs(runner, *others):
    jobs = [*others, *runner.jobs()]
    for job in jobs:
        if not job.cancelling():
            job.cancel()
    await asyncio.gather(*jobs, return_exceptions=True)


async def test_crawl_policy_claim_wait_does_not_block_download_claims(claim_clock):
    download = task(DOWNLOAD_VIDEO, task_id="download-1")
    wait = Wait(180, "采集线路正在冷却", CRAWL, code="crawl_empty_cooldown")
    scheduler = server(download)
    scheduler.claim_with_wait = AsyncMock(side_effect=[([], wait), ([], None)])
    handler = AsyncMock(return_value=None)
    runner = Runner(
        scheduler,
        {CRAWL_CREATOR: handler, DOWNLOAD_VIDEO: handler},
        SLOTS,
        available_identities=lambda: 1,
        publish=AsyncMock(),
        settings=Settings(service_token=TOKEN, crawl_policy_enabled=True),
    )

    await runner._fill()  # 采集预判等待，同一轮继续下载。
    scheduler.claim_with_wait.assert_awaited_once_with({CRAWL_CREATOR: 1})
    scheduler.claim.assert_awaited_once_with({DOWNLOAD_VIDEO: 4})
    assert list(runner.running) == [download.attempt_id]
    await asyncio.gather(*runner.jobs())
    handler.assert_awaited_once_with(download)
    scheduler.defer.assert_not_awaited()

    claim_clock.return_value = 279.99
    await runner._fill()
    scheduler.claim_with_wait.assert_awaited_once()
    scheduler.claim.assert_awaited_with({DOWNLOAD_VIDEO: 4})
    claim_clock.return_value = 280
    await runner._fill()
    assert scheduler.claim_with_wait.await_count == 2
    scheduler.defer.assert_not_awaited()


async def test_crawl_policy_backoff_during_claim_returns_late_crawl_and_starts_download(
    claim_clock,
):
    crawl = task(CRAWL_CREATOR, task_id="crawl-1")
    download = task(DOWNLOAD_VIDEO, task_id="download-1")
    scheduler = server()
    entered, finish_claim = asyncio.Event(), asyncio.Event()

    async def claim(_):
        entered.set()
        await finish_claim.wait()
        return [crawl, download], None

    scheduler.claim_with_wait = AsyncMock(side_effect=claim)
    handler = AsyncMock(return_value=None)
    runner = Runner(
        scheduler,
        {CRAWL_CREATOR: handler, DOWNLOAD_VIDEO: handler},
        SLOTS,
        available_identities=lambda: 2,
        publish=AsyncMock(),
        settings=Settings(service_token=TOKEN, crawl_policy_enabled=True),
    )

    filling = asyncio.create_task(runner._fill())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        wait = Wait(360, "采集线路正在冷却", CRAWL, code="crawl_empty_cooldown")
        runner._pause_crawl(wait)
    finally:
        finish_claim.set()
        await filling
    await asyncio.gather(*runner.jobs())
    handler.assert_awaited_once_with(download)
    scheduler.defer.assert_awaited_once_with(
        crawl.attempt_id,
        30,
        wait.reason,
        False,
        None,
    )


@pytest.mark.parametrize("enabled", [True, False])
async def test_crawl_claim_race_uses_configured_policy_or_legacy_wait(claim_clock, enabled):
    crawl = task(CRAWL_CREATOR, task_id="crawl-1")
    wait = Wait(181, "采集线路正在冷却", CRAWL, code="crawl_empty_cooldown")
    scheduler = server()
    scheduler.claim_with_wait = AsyncMock(return_value=([crawl], None))
    scheduler.claim = AsyncMock(return_value=[crawl])
    handler = AsyncMock(side_effect=wait)
    runner = Runner(
        scheduler,
        {CRAWL_CREATOR: handler},
        SLOTS,
        available_identities=lambda: 2,
        publish=AsyncMock(),
        settings=Settings(service_token=TOKEN, crawl_policy_enabled=enabled),
    )

    await runner._claim({CRAWL_CREATOR: 1})
    await asyncio.gather(*runner.jobs())
    if enabled:
        scheduler.claim_with_wait.assert_awaited_once()
        scheduler.claim.assert_not_awaited()
        assert runner._claim_capacity() == {CRAWL_CREATOR: 0, DOWNLOAD_VIDEO: 4}
        delay = 30
    else:
        scheduler.claim.assert_awaited_once()
        scheduler.claim_with_wait.assert_not_awaited()
        assert runner._claim_capacity() == {CRAWL_CREATOR: 0, DOWNLOAD_VIDEO: 0}
        delay = 181
    scheduler.defer.assert_awaited_once_with(
        crawl.attempt_id,
        delay,
        wait.reason,
        False,
        None,
    )


def events(publish):
    return [entry.args[0] for entry in publish.await_args_list]


@pytest.fixture
def claim_clock(monkeypatch):
    clock = Mock(return_value=100.0)
    monkeypatch.setattr(runner_module, "time", SimpleNamespace(monotonic=clock, time=time.time))
    return clock


async def test_business_retries_use_server_history_and_avoid_failed_lines():
    scheduler, publish = server(), AsyncMock()
    reason = "视频服务器拒绝（403）"
    runner = run_with(scheduler, AsyncMock(side_effect=Retry(reason, "hk")), publish=publish)
    first = task()

    await runner.attempt(first)
    attempt_id, delay, detail, consume, avoid = scheduler.defer.await_args.args
    assert (attempt_id, detail, consume, avoid) == (first.attempt_id, reason, True, "hk")
    assert 48 <= delay <= 72

    second = replace(first, attempt_id=str(uuid4()), attempts=1, history=[f"第 1 次：{reason}"])
    await runner.attempt(second)
    assert 240 <= scheduler.defer.await_args.args[1] <= 360

    third = task(attempts=2, history=[f"第 {i} 次：{reason}" for i in (1, 2)])
    await runner.attempt(third)
    assert scheduler.defer.await_count == 2
    accepted, final = events(publish)[-2:]
    assert accepted["type"] == "accepted" and final["type"] == "download_failed"
    assert final["reason"].count(reason) == 3
    for item in (accepted, final):
        assert item["executor_id"] == scheduler.instance
        assert item["executor_session_id"] == str(scheduler.session_id)
        assert item["attempt_id"] == third.attempt_id


async def test_a_failed_crawl_reports_the_profile_it_read():
    publish = AsyncMock()
    error = Retry("作品列表等待响应超时", "us")
    error.profile = {"handle": "bob"}
    runner = run_with(server(), AsyncMock(side_effect=error), publish=publish)
    await runner.attempt(task(CRAWL_CREATOR, attempts=2))
    final = events(publish)[-1]
    assert (final["conclusion"], final["profile"]) == ("failed", {"handle": "bob"})


@pytest.mark.parametrize(
    ("kind", "wait", "pause"),
    [
        (CRAWL_CREATOR, Wait(30, "没有采集线路：并发已满", CRAWL), 15),
        (DOWNLOAD_VIDEO, Wait(30, "没有采集线路：线路都在冷却或限流", CRAWL), 15),
        (CRAWL_CREATOR, Wait(10, "没有可用身份"), 10),
        (DOWNLOAD_VIDEO, Wait(30, "调度中心连不上"), 15),
    ],
    ids=["crawl-capacity", "crawl-cooldown", "no-identity", "scheduler-unreachable"],
)
async def test_shared_wait_pauses_both_claims_until_deadline(claim_clock, kind, wait, pause):
    blocked = task(kind, "blocked")
    crawl = task(CRAWL_CREATOR, "crawl", "other-batch")
    download = task(DOWNLOAD_VIDEO, "download", "other-batch")
    returning, may_return = asyncio.Event(), asyncio.Event()

    async def defer(*_):
        returning.set()
        await may_return.wait()

    scheduler = server(blocked, crawl, download)
    scheduler.defer.side_effect = defer
    handler = AsyncMock(side_effect=[wait, None, None])
    runner = run_with(scheduler, handler, slots={kind: 1}, available=lambda: 2)
    await runner._fill()
    try:
        [job] = runner.jobs()
        await asyncio.wait_for(returning.wait(), 1)
        runner.slots.update(SLOTS)
        await runner._fill()
        scheduler.claim.assert_awaited_once_with({kind: 1})

        may_return.set()
        await job
        scheduler.defer.assert_awaited_once_with(
            blocked.attempt_id, wait.seconds, wait.reason, False, None
        )
        for _ in range(1, pause):
            claim_clock.return_value += 1
            await runner._fill()
        scheduler.claim.assert_awaited_once()

        claim_clock.return_value += 1
        await runner._fill()
        await asyncio.gather(*runner.jobs())
        assert scheduler.claim.await_args.args == ({CRAWL_CREATOR: 1, DOWNLOAD_VIDEO: 4},)
        assert handler.await_args_list == [call(blocked), call(crawl), call(download)]
    finally:
        await cancel_jobs(runner)


@pytest.mark.parametrize(
    ("seconds", "reason", "pause"),
    [
        (30, "没有下载线路：并发已满", 15),
        (60, "磁盘空间不足", 15),
        (0, "没有下载线路：线路都在冷却或限流", 1),
    ],
    ids=["download-line", "disk-full", "zero-retry-after"],
)
async def test_download_wait_pauses_only_download_claims(claim_clock, seconds, reason, pause):
    blocked = task(task_id="blocked")
    download = task(task_id="download")
    crawl = task(CRAWL_CREATOR, "crawl")
    scheduler = server(blocked, download, crawl)
    handler = AsyncMock(side_effect=[Wait(seconds, reason, DOWNLOAD), None, None])
    runner = run_with(scheduler, handler, slots={DOWNLOAD_VIDEO: 1})
    await runner._fill()
    await asyncio.gather(*runner.jobs())
    scheduler.defer.assert_awaited_once_with(
        blocked.attempt_id, max(seconds, 1), reason, False, None
    )

    runner.slots[CRAWL_CREATOR] = 1
    deadline = claim_clock.return_value + pause
    claim_clock.return_value = deadline - 0.1
    await runner._fill()
    await asyncio.gather(*runner.jobs())
    assert scheduler.claim.await_args_list == [call({DOWNLOAD_VIDEO: 1}), call({CRAWL_CREATOR: 1})]
    assert handler.await_args_list == [call(blocked), call(crawl)]

    claim_clock.return_value = deadline
    await runner._fill()
    await asyncio.gather(*runner.jobs())
    assert scheduler.claim.await_args.args == ({DOWNLOAD_VIDEO: 1},)
    assert handler.await_args_list == [call(blocked), call(crawl), call(download)]


async def test_wait_during_claim_blocks_the_fallback_claim(claim_clock):
    blocked = task()
    wait_now, claiming, may_reply = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def handler(_):
        await wait_now.wait()
        raise Wait(30, "没有下载线路：并发已满", DOWNLOAD)

    async def claim(_):
        claiming.set()
        await may_reply.wait()
        return []

    scheduler = server(blocked)
    runner = run_with(scheduler, handler, slots={DOWNLOAD_VIDEO: 2})
    await runner._fill()
    [job] = runner.jobs()
    runner.slots[CRAWL_CREATOR] = 1
    scheduler.claim.side_effect = claim
    in_transit = asyncio.create_task(runner._fill())
    try:
        await asyncio.wait_for(claiming.wait(), 1)
        wait_now.set()
        await job
        may_reply.set()
        await asyncio.wait_for(in_transit, 1)
        assert scheduler.claim.await_args_list == [
            call({DOWNLOAD_VIDEO: 2}),
            call({CRAWL_CREATOR: 1}),
        ]
    finally:
        await cancel_jobs(runner, in_transit)


async def test_claim_backs_off_when_the_last_idle_identity_is_lost_in_transit(claim_clock):
    active = task()
    available = 1

    async def claim(_):
        nonlocal available
        available = 0
        return [active]

    scheduler, handler = server(), AsyncMock()
    scheduler.claim.side_effect = claim
    runner = run_with(scheduler, handler, slots={DOWNLOAD_VIDEO: 1}, available=lambda: available)
    await runner._fill()
    await asyncio.gather(*runner.jobs())
    handler.assert_not_awaited()
    scheduler.defer.assert_awaited_once_with(
        active.attempt_id, 1, "本机身份或执行名额已变更", False, None
    )

    available = 1
    await runner._fill()
    scheduler.claim.assert_awaited_once()
    claim_clock.return_value += 5
    scheduler.claim.side_effect = None
    scheduler.claim.return_value = []
    await runner._fill()
    assert scheduler.claim.await_count == 2


async def test_claim_loop_backs_off_after_a_connection_failure(claim_clock):
    polled = asyncio.Queue()
    recovered = asyncio.Event()

    def available():
        polled.put_nowait(None)
        return 1

    async def claim(_):
        if scheduler.claim.await_count == 1:
            raise httpx.ConnectError("unavailable")
        recovered.set()
        return []

    scheduler = server()
    scheduler.claim.side_effect = claim
    runner = run_with(scheduler, AsyncMock(), available=available)
    loop = asyncio.create_task(runner.run())
    try:
        await asyncio.wait_for(polled.get(), 1)
        runner.wake()
        await asyncio.wait_for(polled.get(), 1)
        scheduler.claim.assert_awaited_once()

        claim_clock.return_value += 5
        runner.wake()
        await asyncio.wait_for(recovered.wait(), 1)
    finally:
        await cancel_jobs(runner, loop)


async def test_a_deleted_video_ends_at_once():
    scheduler, publish = server(), AsyncMock()
    runner = run_with(scheduler, AsyncMock(side_effect=Gone("视频已删除或不存在")), publish=publish)
    await runner.attempt(task())
    assert events(publish)[-1]["reason"] == "视频已删除或不存在"
    scheduler.defer.assert_not_awaited()


async def test_five_failed_downloads_pause_the_device():
    scheduler = server()
    runner = run_with(scheduler, AsyncMock(side_effect=Retry("连不上视频服务器")))
    for number in range(5):
        await runner.attempt(task(task_id=f"download-{number}"))
    assert runner.paused_until > time.time() + 50
    await runner._fill()
    assert DOWNLOAD_VIDEO not in scheduler.claim.await_args.args[0]


async def test_one_idle_identity_alternates_between_crawl_and_download():
    scheduler = server(
        *(task(CRAWL_CREATOR, f"crawl-{i}") for i in range(4)),
        *(task(DOWNLOAD_VIDEO, f"download-{i}") for i in range(16)),
    )
    handler = AsyncMock(return_value=None)
    runner = run_with(scheduler, handler)

    for count in (1, 4) * 4:
        await runner._fill()
        assert len(runner.jobs()) == count
        await asyncio.gather(*runner.jobs())
    assert [entry.args[0].kind for entry in handler.await_args_list] == [
        CRAWL_CREATOR,
        DOWNLOAD_VIDEO,
        DOWNLOAD_VIDEO,
        DOWNLOAD_VIDEO,
        DOWNLOAD_VIDEO,
    ] * 4
    assert (
        scheduler.claim.await_args_list == [call({CRAWL_CREATOR: 1}), call({DOWNLOAD_VIDEO: 4})] * 4
    )


async def test_one_identity_claims_from_the_other_kind_when_preferred_kind_is_empty():
    scheduler = server(task())
    runner = run_with(scheduler, AsyncMock(return_value=None))
    await runner._fill()
    await asyncio.gather(*runner.jobs())
    assert [entry.args[0] for entry in scheduler.claim.await_args_list] == [
        {CRAWL_CREATOR: 1},
        {DOWNLOAD_VIDEO: 4},
    ]


async def test_runner_pipelines_more_transfers_than_identities_and_allows_crawl(
    tmp_path, monkeypatch
):
    scheduler = server(
        *(task(CRAWL_CREATOR, f"crawl-{i}") for i in range(2)),
        *(task(DOWNLOAD_VIDEO, f"download-{i}") for i in range(4)),
    )
    scheduler.lease = AsyncMock(return_value=Lease("lease", "route", {"kind": "direct"}))
    scheduler.release = AsyncMock()
    scheduler.upload = AsyncMock(return_value=204)
    browsers = Browsers("chrome")
    pool = IdentityPool(
        Settings(service_token=TOKEN, profile_root=tmp_path), Mock(), scheduler, browsers
    )
    identity = Identity(
        "ready",
        browser="157",
        warmed_at=time.time(),
        session=Session({"tt_chain_token": "test-token"}, "ua", "157"),
    )
    pool.browser = identity.browser
    pool.identities[identity.id] = identity
    downloader = Downloader(pool, scheduler, browsers, tmp_path / "videos")
    lookup, transfer, crawl = asyncio.Queue(), asyncio.Queue(), asyncio.Event()
    may_resolve, may_finish = asyncio.Event(), asyncio.Event()

    async def address(_, current, borrowed, session):
        assert borrowed is identity and identity.busy and session is identity.session
        lookup.put_nowait(current.task_id)
        await may_resolve.wait()
        return ["https://v16.tiktokcdn.com/video"]

    async def fetch(_, credentials, referer, candidates, path, progress):
        assert credentials == ("ua", "test-token")
        transfer.put_nowait(progress.task_id)
        await may_finish.wait()
        return "digest"

    async def collect(_):
        assert pool.take() is identity
        try:
            crawl.set()
            await may_finish.wait()
        finally:
            pool.give_back(identity)

    monkeypatch.setattr("video_crawler.download.MIN_FREE", 0)
    monkeypatch.setattr(downloader, "_address", address)
    monkeypatch.setattr(downloader, "_fetch", fetch)
    runner = Runner(
        scheduler,
        {DOWNLOAD_VIDEO: downloader, CRAWL_CREATOR: collect},
        {DOWNLOAD_VIDEO: 3},
        available_identities=lambda: len(pool.available()),
        publish=AsyncMock(),
    )

    await runner._fill()
    try:
        assert await asyncio.wait_for(lookup.get(), 1) == "download-0"
        assert lookup.empty() and identity.busy
        scheduler.claim.assert_awaited_once_with({DOWNLOAD_VIDEO: 3})
        assert runner.busy(DOWNLOAD_VIDEO) == 3
        may_resolve.set()
        transferring = {await asyncio.wait_for(transfer.get(), 1) for _ in range(3)}
        assert transferring == {"download-0", "download-1", "download-2"}
        assert pool.available() == [identity]

        runner.slots[CRAWL_CREATOR] = 2
        await runner._fill()
        await asyncio.wait_for(crawl.wait(), 1)
        assert runner.busy(DOWNLOAD_VIDEO) == 3 and runner.busy(CRAWL_CREATOR) == 1
        assert identity.busy
        assert scheduler.claim.await_args_list == [
            call({DOWNLOAD_VIDEO: 3}),
            call({CRAWL_CREATOR: 1}),
        ]
        may_finish.set()
        await asyncio.gather(*runner.jobs())
        scheduler.defer.assert_not_awaited()
        assert scheduler.upload.await_count == 3
    finally:
        await cancel_jobs(runner)


async def test_available_cache_lets_runner_claim_with_no_idle_identity():
    cached = task(task_id="cached")
    uncached = task(task_id="uncached")
    uncached.body["work"]["work_id"] = "8"
    scheduler = server(task(CRAWL_CREATOR, "crawl"), cached, uncached)
    handler = AsyncMock(return_value=None)
    cache = tiktok.MediaCache()
    cache.remember("busy-profile", "7", ["https://v16.tiktokcdn.com/video"])

    def cached_download(work_id):
        return (
            cache.has_usable(lambda _: True) if work_id is None else cache.get(work_id) is not None
        )

    runner = run_with(scheduler, handler, available=lambda: 0, cached_download=cached_download)
    await runner._fill()
    await asyncio.gather(*runner.jobs())

    scheduler.claim.assert_awaited_once_with({DOWNLOAD_VIDEO: 4})
    handler.assert_awaited_once_with(cached)
    scheduler.defer.assert_awaited_once_with(
        uncached.attempt_id, 1, "本机身份或执行名额已变更", False, None
    )


async def test_heartbeat_changes_slots_and_cancels_the_revoked_attempt():
    active = task()
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def handler(current):
        assert current is active
        started.set()
        try:
            await asyncio.Future()
        finally:
            cleaned.set()

    scheduler, publish = server(active), AsyncMock()
    scheduler.register = AsyncMock(
        return_value={
            "crawl_slots": 3,
            "download_slots": 4,
            "warm_slots": 1,
            "identity_target": 6,
            "revoked_attempts": [active.attempt_id, "unknown-attempt"],
        }
    )
    pool = SimpleNamespace(snapshot=Mock(return_value={"ready": 1}), configure=Mock(), warm_slots=0)
    browsers = Browsers("chrome")
    runner = run_with(scheduler, handler, publish=publish)
    await runner._fill()
    await asyncio.wait_for(started.wait(), 1)
    beat = asyncio.create_task(heartbeat(scheduler, "http://crawler", runner, pool, browsers))
    try:
        await asyncio.wait_for(cleaned.wait(), 1)
        await asyncio.gather(*runner.jobs(), return_exceptions=True)
        assert scheduler.register.await_args.args[-1] == {active.attempt_id}
        assert scheduler.register.await_args.args[-2] == {"ready": 1}
        assert runner.attempt_ids() == set()
        assert runner.slots[CRAWL_CREATOR] == 3 and browsers.limit == 4
        pool.configure.assert_called_once_with(6, 1)
        assert [event["type"] for event in events(publish)] == ["accepted"]
        scheduler.defer.assert_not_awaited()
    finally:
        await cancel_jobs(runner, beat)


async def test_control_api_pauses_crawl_and_cancels_download_for_the_same_batch():
    crawl, download = task(CRAWL_CREATOR, "t2"), task()
    started, cleaned = asyncio.Queue(), asyncio.Queue()
    returned = asyncio.Event()

    async def handler(current):
        started.put_nowait(current)
        try:
            await asyncio.Future()
        finally:
            cleaned.put_nowait(current)

    async def defer(*_):
        returned.set()

    scheduler = server(crawl, download)
    scheduler.defer.side_effect = defer
    runner = run_with(scheduler, handler, available=lambda: 2)
    pool = SimpleNamespace(summary=Mock(return_value={"ready": 2}))
    app = control_api(TOKEN, runner, pool, scheduler)
    await runner._fill()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://crawler"
    ) as api:
        try:
            assert [await asyncio.wait_for(started.get(), 1) for _ in range(2)] == [crawl, download]
            assert (await api.get("/health")).status_code == 401
            api.headers["Authorization"] = f"Bearer {TOKEN}"
            assert (await api.get("/api/health")).json() == {"status": "ok"}
            assert (await api.get("/progress")).json() == {"crawls": [], "downloads": []}
            assert (await api.post("/api/batches/b1/pause")).status_code == 204
            assert await asyncio.wait_for(cleaned.get(), 1) is crawl
            await asyncio.wait_for(returned.wait(), 1)
            scheduler.defer.assert_awaited_once_with(crawl.attempt_id, 1, "批次已暂停", False, None)
            status = (await api.get("/api/status")).json()
            assert status["paused_batches"] == ["b1"]
            assert status["download"] == {"running": 1, "queued": 0}
            assert (await api.post("/batches/b1/resume")).status_code == 204
            assert not runner.paused_batches
            assert (await api.post("/api/batches/b1/cancel")).status_code == 204
            assert await asyncio.wait_for(cleaned.get(), 1) is download
        finally:
            await cancel_jobs(runner)


async def test_batch_cancel_ignores_a_claim_reply_already_in_transit():
    active = task()
    sent, response = asyncio.Event(), asyncio.Event()

    async def claim(_):
        sent.set()
        await response.wait()
        return [active]

    scheduler, handler = server(), AsyncMock()
    scheduler.claim.side_effect = claim
    runner = run_with(scheduler, handler, slots={DOWNLOAD_VIDEO: 1})
    in_transit = asyncio.create_task(runner._fill())
    try:
        await asyncio.wait_for(sent.wait(), 1)
        runner.cancel(batch_id=active.batch_id)
        response.set()
        await asyncio.wait_for(in_transit, 1)
        assert not runner.running
        handler.assert_not_awaited()
    finally:
        await cancel_jobs(runner, in_transit)


async def test_new_claim_uses_server_batch_state_when_resume_notice_was_missed():
    crawl = task(CRAWL_CREATOR)
    handler = AsyncMock(return_value={"conclusion": "ok"})
    runner = run_with(server(crawl), handler)
    runner.pause(crawl.batch_id)

    await runner._fill()
    await asyncio.gather(*runner.jobs())
    handler.assert_awaited_once_with(crawl)
    assert not runner.paused_batches


async def test_revocation_during_timeout_cleanup_lets_cleanup_finish_without_retrying(monkeypatch):
    monkeypatch.setitem(runner_module.LIMITS, DOWNLOAD_VIDEO, 0.01)
    active = task()
    closing, may_close = asyncio.Event(), asyncio.Event()

    async def handler(_):
        try:
            await asyncio.Future()
        finally:
            closing.set()
            await may_close.wait()

    scheduler, publish = server(active), AsyncMock()
    runner = run_with(scheduler, handler, publish=publish)
    await runner._fill()
    try:
        [job] = runner.jobs()
        await asyncio.wait_for(closing.wait(), 1)
        assert runner.attempt_ids() == {active.attempt_id}
        runner.revoke([active.attempt_id])
        assert job.cancelling() == 1
        may_close.set()
        await asyncio.wait_for(asyncio.gather(job, return_exceptions=True), 1)
        assert job.cancelled()
        assert [event["type"] for event in events(publish)] == ["accepted"]
        scheduler.defer.assert_not_awaited()
    finally:
        may_close.set()
        await cancel_jobs(runner)


async def test_redis_retry_preserves_the_result_id_and_keeps_the_attempt_in_heartbeat():
    active = task(CRAWL_CREATOR)
    sent = []
    failed = asyncio.Event()
    loop = asyncio.get_running_loop()

    def enqueue(message):
        assert (message.queue_name, message.actor_name) == ("crawler.events", "crawler_event")
        [item] = message.args
        sent.append(item)
        if item["type"] == "crawled" and not failed.is_set():
            loop.call_soon_threadsafe(failed.set)
            raise RedisConnectionError("unavailable")

    broker = SimpleNamespace(enqueue=enqueue)
    runner = run_with(
        server(active),
        AsyncMock(return_value={"conclusion": "private"}),
        publish=partial(publish_event, broker),
    )
    await runner._fill()
    try:
        [job] = runner.jobs()
        await asyncio.wait_for(failed.wait(), 1)
        assert runner.attempt_ids() == {active.attempt_id}
        await asyncio.wait_for(job, 3)
        assert [item["type"] for item in sent] == ["accepted", "crawled", "crawled"]
        assert sent[-2] == sent[-1]
    finally:
        await cancel_jobs(runner)


async def test_download_progress_and_identity_are_released_when_control_cancels(
    tmp_path, monkeypatch
):
    monkeypatch.setattr("video_crawler.download.MIN_FREE", 0)
    started = asyncio.Event()

    async def wait_for_line(*args, **kwargs):
        started.set()
        await asyncio.Future()

    active = task()
    settings = Settings(service_token=TOKEN, profile_root=tmp_path)
    scheduler = server(active)
    scheduler.lease = wait_for_line
    browsers = Browsers("chrome")
    pool = IdentityPool(settings, Mock(), scheduler, browsers)
    identity = Identity(
        "ready",
        browser="157",
        warmed_at=time.time(),
        session=Session({"tt_chain_token": "test-token"}, "ua", "157"),
    )
    pool.browser = identity.browser
    pool.identities[identity.id] = identity
    downloader = Downloader(pool, scheduler, browsers, tmp_path / "videos")
    runner = run_with(scheduler, downloader)
    app = control_api(
        TOKEN,
        runner,
        pool,
        scheduler,
        downloads=downloader.progress,
        download_speed=downloader.download_speed,
    )
    await runner._fill()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://crawler"
    ) as api:
        try:
            await asyncio.wait_for(started.wait(), 1)
            [job] = runner.jobs()
            assert identity.busy
            assert (await api.get("/api/download-progress")).status_code == 401
            api.headers["Authorization"] = f"Bearer {TOKEN}"
            response = await api.get("/api/download-progress")
            assert response.headers["X-Download-Bytes-Per-Second"] == "0"
            assert response.json() == [
                {
                    "task_id": "t1",
                    "work_id": "7",
                    "stage": "preparing",
                    "received_bytes": 0,
                    "total_bytes": None,
                }
            ]
            assert (await api.post("/api/tasks/t1/cancel")).status_code == 204
            await asyncio.gather(job, return_exceptions=True)
            assert (await api.get("/api/download-progress")).json() == []
            assert pool.available() == [identity]
        finally:
            await cancel_jobs(runner)


async def test_the_chrome_limit_can_grow_while_running():
    browsers = Browsers("chrome")
    await browsers.resize(1)

    async def another():
        async with browsers._slot():
            pass

    async with browsers._slot():
        waiting = asyncio.create_task(another())
        await asyncio.sleep(0.05)
        assert not waiting.done()
        await browsers.resize(2)
        await asyncio.wait_for(waiting, 1)
