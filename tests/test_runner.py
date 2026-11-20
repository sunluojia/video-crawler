"""本地状态表和任务队列的规则：去重、暂停取消、重试、环境问题不算次数。"""

import asyncio
import time

import httpx
import pytest

from video_crawler.api import control_api
from video_crawler.browser import Browsers
from video_crawler.errors import Gone, Retry, Wait
from video_crawler.identities import IdentityPool
from video_crawler.runner import CRAWL_CREATOR, DOWNLOAD_VIDEO, Runner, event
from video_crawler.scheduler import CRAWL, Scheduler
from video_crawler.settings import Settings
from video_crawler.store import Store

SLOTS = {CRAWL_CREATOR: 2, DOWNLOAD_VIDEO: 4}
TOKEN = "t" * 16


@pytest.fixture
def store(tmp_path) -> Store:
    return Store(tmp_path / "state.db")


def body(task_id="t1", run=1, batch_id="b1") -> dict:
    return {"task_id": task_id, "run": run, "batch_id": batch_id, "work": {"work_id": "7"}}


def receive(store: Store, kind=DOWNLOAD_VIDEO, **fields) -> bool:
    task = body(**fields)
    return store.receive(kind, task, event(task["task_id"], task["run"], "accepted"))


def due(store: Store, kind=DOWNLOAD_VIDEO):
    return store.due(kind, time.time() + 3600, set())


def types(store: Store) -> list[str]:
    return [item["type"] for item in store.events()]


def test_duplicate_and_older_runs_are_ignored_and_newer_runs_replace(store):
    assert receive(store)
    assert not receive(store)
    assert receive(store, run=2)
    assert not receive(store, run=1)
    assert due(store).run == 2
    assert types(store) == ["accepted", "accepted"]


def test_paused_batches_wait_and_cancelled_batches_are_dropped(store):
    receive(store, task_id="t1", batch_id="b1")
    store.pause("b1")
    assert due(store) is None
    store.resume("b1")
    assert due(store).task_id == "t1"
    store.cancel_batch("b1")
    assert due(store) is None
    assert not receive(store, task_id="t2", batch_id="b1")  # 取消后迟到的任务


def runner_with(store: Store, outcome) -> Runner:
    async def attempt(_):
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return Runner(store, {CRAWL_CREATOR: attempt, DOWNLOAD_VIDEO: attempt}, SLOTS)


async def test_three_failed_attempts_report_every_reason(store):
    receive(store)
    runner = runner_with(store, Retry("视频服务器拒绝（403）", "1.2.3.4"))

    await runner.attempt(due(store))
    task = due(store)
    assert (task.attempts, task.avoid) == (1, ["1.2.3.4"])
    assert 48 <= task.next_at - time.time() <= 72  # 1 分钟后，加减 20%

    await runner.attempt(task)
    await runner.attempt(due(store))
    assert due(store) is None
    [*_, final] = store.events()
    assert final["type"] == "download_failed"
    assert final["reason"].count("视频服务器拒绝（403）") == 3


async def test_waiting_does_not_count_and_holds_back_crawls(store):
    receive(store)
    runner = runner_with(store, Wait(30, "没有采集线路：线路都在冷却或限流", CRAWL))

    await runner.attempt(due(store))
    task = due(store)
    assert task.attempts == 0
    assert task.waiting == "没有采集线路：线路都在冷却或限流"
    assert "t1" in runner.starved  # 有下载在等采集线路，先不开始新的获取作品

    runner.forget("b1")  # 批次暂停后不再挡着
    assert not runner.starved


async def test_a_deleted_video_ends_at_once(store):
    receive(store)
    await runner_with(store, Gone("视频已删除或不存在")).attempt(due(store))
    assert due(store) is None
    assert store.events()[-1]["reason"] == "视频已删除或不存在"


async def test_a_crawl_result_is_reported(store):
    receive(store, kind=CRAWL_CREATOR)
    fields = {"conclusion": "private", "profile": {"handle": "bob"}}
    await runner_with(store, fields).attempt(due(store, CRAWL_CREATOR))
    final = store.events()[-1]
    assert (final["type"], final["conclusion"], final["profile"]) == (
        "crawled",
        "private",
        fields["profile"],
    )


async def test_five_failed_downloads_in_a_row_pause_the_device(store):
    runner = runner_with(store, Retry("连不上视频服务器"))
    for number in range(5):
        receive(store, task_id=f"t{number}")
        await runner.attempt(store.due(DOWNLOAD_VIDEO, time.time(), set()))
    assert runner.paused_until > time.time() + 50


async def test_slots_from_the_heartbeat_take_effect_without_restart(store, tmp_path):
    started = asyncio.Event()

    async def attempt(_):
        started.set()
        await asyncio.sleep(10)

    nothing_yet = {CRAWL_CREATOR: 0, DOWNLOAD_VIDEO: 0}
    runner = Runner(store, {CRAWL_CREATOR: attempt, DOWNLOAD_VIDEO: attempt}, nothing_yet)
    receive(store, CRAWL_CREATOR, task_id="t1")
    receive(store, CRAWL_CREATOR, task_id="t2")
    loop = asyncio.create_task(runner.run())
    await asyncio.sleep(0.05)
    assert not runner.running  # 第一次心跳拿到并发数之前不开始

    runner.slots[CRAWL_CREATOR] = 1
    runner.wake()
    await asyncio.wait_for(started.wait(), 1)
    assert runner.busy(CRAWL_CREATOR) == 1

    # 执行程序页上看到的：1 个在做，1 个排队
    settings = Settings(service_token=TOKEN, profile_root=tmp_path)
    scheduler = Scheduler(settings)
    pool = IdentityPool(settings, store, scheduler, Browsers("chrome"))
    app = control_api(TOKEN, store, runner, pool, scheduler)
    headers = {"Authorization": f"Bearer {TOKEN}"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), headers=headers) as client:
        status = (await client.get("http://crawler/api/status")).json()
    assert status["crawl"] == {"running": 1, "queued": 1}

    jobs = [loop, *(job for _, job in runner.running.values())]
    for job in jobs:
        job.cancel()
    await asyncio.gather(*jobs, return_exceptions=True)


async def test_the_chrome_limit_can_grow_while_running():
    browsers = Browsers("chrome")
    await browsers.resize(1)

    async def another():
        async with browsers._slot():
            pass

    async with browsers._slot():
        waiting = asyncio.create_task(another())
        await asyncio.sleep(0.05)
        assert not waiting.done()  # 满了就等
        await browsers.resize(2)
        await asyncio.wait_for(waiting, 1)
