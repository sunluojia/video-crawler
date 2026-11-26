"""身份池的快照和账号池页面的操作，见爬取方案第 6.3 节。"""

import asyncio
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient

from video_crawler import identities
from video_crawler.api import control_api
from video_crawler.browser import Browsers, Session
from video_crawler.errors import Wait
from video_crawler.identities import Identity, IdentityPool
from video_crawler.runner import Runner
from video_crawler.scheduler import Lease, Scheduler
from video_crawler.settings import Settings
from video_crawler.store import IdentityStore

TOKEN = "t" * 16


@pytest.fixture
def pool(tmp_path) -> IdentityPool:
    """调度中心分给本机 3 个。"""
    settings = Settings(service_token=TOKEN, profile_root=tmp_path)
    store = Mock(spec=IdentityStore)
    pool = IdentityPool(settings, store, Scheduler(settings), Browsers("chrome"))
    pool.target = 3
    pool.browser = "146"
    return pool


def control(pool: IdentityPool) -> TestClient:
    runner = Runner(pool._scheduler, {}, {}, available_identities=lambda: 0, publish=AsyncMock())
    app = control_api(TOKEN, runner, pool, pool._scheduler)
    return TestClient(app, headers={"Authorization": f"Bearer {TOKEN}"})


def add(pool: IdentityPool, name: str, **fields) -> Identity:
    """放进池里一个身份；warmed 为真时带着刚预热好的 token。"""
    warmed = fields.pop("warmed", True)
    identity = Identity(name, browser="146", **fields)
    if warmed:
        cookies = {"tt_chain_token": "TOKEN-VALUE", "sessionid": "COOKIE-VALUE"}
        identity.session = Session(cookies, "ua", "146")
        identity.warmed_at = time.time()
    (pool.profile(identity)).mkdir()
    pool.identities[name] = identity
    return identity


def test_snapshot_shows_states_and_recent_rates_without_cookies(pool):
    add(pool, "ready")
    add(pool, "busy", busy=True)
    add(pool, "cooling", fails=1, cooldown_until=time.time() + 60, warmed=False)
    add(pool, "stale", warmed=False)
    pool.retire(add(pool, "old"), "下载连续 6 次被拒")
    for name, ok in [("warm", True), ("warm", False), ("list", False), ("download", True)]:
        pool.note(name, ok)

    snapshot = pool.snapshot()

    assert {key: snapshot[key] for key in ("target", "ready", "in_use", "cooling", "total")} == {
        "target": 3,
        "ready": 2,
        "in_use": 1,
        "cooling": 1,
        "total": 4,
    }
    assert (snapshot["warm_total"], snapshot["warm_failed"]) == (2, 1)
    assert (snapshot["list_total"], snapshot["list_empty"]) == (1, 1)
    assert (snapshot["download_total"], snapshot["download_blocked"]) == (1, 0)
    states = {item["id"]: item["state"] for item in snapshot["items"]}
    assert states == {
        "ready": "ready",
        "busy": "in_use",
        "cooling": "cooling",
        "stale": "stale",
        "old": "retired",
    }
    stale = next(item for item in snapshot["items"] if item["id"] == "stale")
    assert stale["warmed_at"] is None and stale["token_expires_at"] is None
    assert "VALUE" not in str(snapshot)  # Cookie 和 token 不出设备

    since = snapshot["short_since"]  # 可用 2 个，少于目标 3 个
    assert since and pool.snapshot()["short_since"] == since  # 一直不够就一直是开始的时间
    pool.identities["stale"].session = pool.identities["ready"].session
    pool.identities["stale"].warmed_at = time.time()
    assert pool.snapshot()["short_since"] is None  # 补够了马上清掉


def test_the_pool_tops_up_to_what_the_scheduler_assigns(pool):
    pool.target = 0
    assert pool._next() is None  # 第一次心跳拿到数量之前不新建
    pool.target = 2
    blocked = add(pool, "a", fails=3)
    assert pool._next() is not None  # 连续失败的不计入，补新的
    for name in "bcd":
        add(pool, name, fails=3)
    assert pool._next() is None  # 最多保留目标的两倍
    blocked.warmed_at = 0
    blocked.cooldown_until = time.time() + 60
    assert pool._next() is None
    blocked.cooldown_until = 0
    assert pool._next() is blocked  # 无法新建时，冷却结束的资料仍能恢复


def test_take_prefers_the_exact_cached_identity_only_if_it_is_available(pool):
    first = add(pool, "first")
    preferred = add(pool, "second", fails=1)
    assert pool.take(preferred_id=preferred.id) is preferred
    assert pool.take(preferred_id=preferred.id) is first


async def test_identity_metadata_is_restored_without_a_session_and_persisted_without_cookies(pool):
    existing = Identity("a123456789ab", fails=2)
    orphan = "b123456789ab"
    pool._root.joinpath(existing.id).mkdir()
    pool._root.joinpath(orphan).mkdir()
    pool._store.records.return_value = [existing.record(), Identity("../unsafe").record()]

    await pool.restore()
    assert set(pool.identities) == {existing.id, orphan}
    ready = pool.identities[existing.id]
    assert ready.fails == 2 and ready.session is None
    pool.refreshed(ready, Session({"tt_chain_token": "test-token"}, "ua", "146"))
    pool.give_back(ready)
    await pool.persist()

    [saved] = pool._store.save.call_args.args[0]
    assert saved == ready.record()
    assert {"session", "token", "cookies"}.isdisjoint(saved)
    assert pool._dirty == {}


def test_refill_recovers_expired_profiles_before_refreshing_live_sessions(pool):
    expiring = add(pool, "expiring")
    expiring.warmed_at -= identities.REFRESH_AFTER
    expired = add(pool, "expired")
    expired.warmed_at -= identities.TOKEN_TTL
    busy = add(pool, "busy", busy=True)
    busy.warmed_at -= identities.TOKEN_TTL
    add(pool, "cooling", warmed=False, fails=1, cooldown_until=time.time() + 60)

    assert pool._next() is expired
    pool.refreshed(expired, expiring.session)

    created = pool._next()
    assert created is not None and created.id not in pool.identities
    created.browser = pool.browser
    pool.refreshed(created, expiring.session)
    pool.identities[created.id] = created
    assert pool._next() is expiring


async def test_success_and_blocked_return_refill_without_waiting_for_poll(pool, monkeypatch):
    monkeypatch.setattr(identities, "CHECK_EVERY", 60)
    completed = asyncio.Queue()

    async def warm(identity: Identity) -> bool:
        identity.browser = pool.browser
        pool.refreshed(identity, Session({"tt_chain_token": "test"}, "ua", "146"))
        pool.identities[identity.id] = identity
        completed.put_nowait(identity)
        return True

    monkeypatch.setattr(pool, "_warm", warm)
    pool.configure(3, 1)
    loop = asyncio.create_task(pool.run())
    try:
        first = await asyncio.wait_for(completed.get(), 1)
        await asyncio.wait_for(completed.get(), 1)
        await asyncio.wait_for(completed.get(), 1)
        assert pool.summary()["ready"] == 3

        assert pool.take() is first
        pool.give_back(first, "blocked")
        replacement = await asyncio.wait_for(completed.get(), 1)
        assert replacement.id != first.id
        assert first.cooldown_until > time.time()
        assert pool.summary()["ready"] == 3
    finally:
        loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop


async def test_repeated_empty_listings_cool_and_rewarm_only_the_affected_identity(
    pool, monkeypatch
):
    pool.target = 2
    affected = add(pool, "affected")
    healthy = add(pool, "healthy")
    session = affected.session
    for count in range(1, identities.COUNTED_FAILS):
        assert pool.take(preferred_id=affected.id) is affected
        pool.give_back(affected, "empty_response")
        assert affected.listing_fails == count and affected.cooldown_until == 0
        assert affected.session is session and pool.summary()["ready"] == 2

    assert pool.take() is healthy
    pool.give_back(healthy)
    assert pool.take(preferred_id=affected.id) is affected
    pool.give_back(affected, "empty_response")
    state = next(item for item in pool.snapshot()["items"] if item["id"] == affected.id)
    assert state["state"] == "cooling" and state["fails"] == identities.COUNTED_FAILS
    assert affected.session is None
    assert pool.available() == [healthy]
    assert pool.profile(affected).is_dir()
    replacement = pool._next()
    assert replacement is not None and replacement.id not in pool.identities

    affected.cooldown_until = time.time() - 1
    assert pool._next() is affected
    lease = Lease("lease", "proxy:test", {"kind": "proxy"})
    monkeypatch.setattr(pool._scheduler, "lease", AsyncMock(return_value=lease))
    monkeypatch.setattr(pool._scheduler, "release", AsyncMock())

    @asynccontextmanager
    async def open_browser(profile, route, *, warming):
        assert (profile, route, warming) == (pool.profile(affected), lease.route, True)
        yield SimpleNamespace(goto=AsyncMock(), wait_for_selector=AsyncMock())

    monkeypatch.setattr(pool._browsers, "open", open_browser)
    renewed = Session({"tt_chain_token": "renewed"}, "ua", "146")
    monkeypatch.setattr(identities, "session_of", AsyncMock(return_value=renewed))
    assert await pool._warm(affected)
    assert affected.session is renewed and affected.listing_fails == 0
    assert pool.available() == [affected, healthy]


def test_listing_results_are_independent_of_download_feedback(pool):
    identity = add(pool, "shared", fails=1)
    token = identity.session.token
    pool.take()
    pool.give_back(identity, "empty_response")
    pool.record_download(identity.id, blocked=False, succeeded=True, token=token)
    assert identity.fails == 0 and identity.listing_fails == 1

    pool.take()
    pool.give_back(identity, "neutral")
    pool.take()
    pool.give_back(identity, "ok")
    assert identity.listing_fails == 1

    pool.take()
    pool.give_back(identity, "listing_ok")
    assert identity.listing_fails == 0


def test_empty_listings_never_advance_the_download_retirement_counter(pool):
    identity = add(pool, "shared", fails=identities.RETIRE_FAILS - 1)
    for _ in range(identities.COUNTED_FAILS):
        pool.take()
        pool.give_back(identity, "empty_response")

    assert identity.fails == identities.RETIRE_FAILS - 1
    assert pool.identities[identity.id] is identity
    assert pool.profile(identity).is_dir()


def test_delayed_download_feedback_preserves_the_current_borrow_and_retirement(pool):
    identity = add(pool, "shared", fails=identities.RETIRE_FAILS - 2)
    token = identity.session.token
    assert pool.take() is identity

    pool.record_download(identity.id, blocked=True, token=token)
    first_cooldown = identity.cooldown_until
    pool.record_download(identity.id, blocked=True, token=token)
    pool.record_download(identity.id, blocked=False, succeeded=True, token=token)

    assert identity.busy and identity.session is None
    assert identity.fails == identities.RETIRE_FAILS
    assert identity.cooldown_until > first_cooldown
    assert pool.profile(identity).is_dir()
    pool.give_back(identity, "ok")
    assert identity.id not in pool.identities and not pool.profile(identity).exists()

    pool.record_download(identity.id, blocked=True, token=token)
    snapshot = pool.snapshot()
    assert (snapshot["download_total"], snapshot["download_blocked"]) == (4, 3)


async def test_a_download_waiting_for_an_identity_times_out_as_a_resource_wait(pool, monkeypatch):
    monkeypatch.setattr(identities, "DOWNLOAD_IDENTITY_WAIT", 0)
    identity = add(pool, "busy")
    assert pool.take() is identity
    with pytest.raises(Wait, match="等待身份取链超时") as error:
        await pool.take_for_download()
    assert error.value.seconds == 10 and identity.busy


async def test_failed_warm_gives_another_profile_a_chance(pool, monkeypatch):
    monkeypatch.setattr(identities, "CHECK_EVERY", 0)
    failing = add(pool, "failing", warmed=False, busy=True)
    other = add(pool, "other", warmed=False)
    pool._warming[failing.id] = failing
    monkeypatch.setattr(pool, "_warm", AsyncMock(return_value=False))

    await pool._attempt(failing)

    assert failing.warm_retry_at > time.time()
    assert pool._next() is other
    failing.warm_retry_at = 0
    assert pool._next() is failing


async def test_warming_can_resize_and_reserves_each_profile_and_target_slot(pool, monkeypatch):
    monkeypatch.setattr(identities, "CHECK_EVERY", 0.01)
    stale = add(pool, "stale", warmed=False)
    started = asyncio.Queue()
    gates = {}

    async def warm(identity: Identity) -> bool:
        gates[identity.id] = asyncio.Event()
        started.put_nowait(identity)
        await gates[identity.id].wait()
        identity.browser = pool.browser
        pool.refreshed(identity, Session({"tt_chain_token": "test"}, "ua", "146"))
        pool.identities[identity.id] = identity
        return True

    monkeypatch.setattr(pool, "_warm", warm)
    loop = asyncio.create_task(pool.run())
    try:
        pool.configure(3, 1)
        assert (await asyncio.wait_for(started.get(), 1)) is stale
        assert pool.snapshot()["warming"] == 1

        pool.configure(3, 4)
        second = await asyncio.wait_for(started.get(), 1)
        third = await asyncio.wait_for(started.get(), 1)
        snapshot = pool.snapshot()
        assert snapshot["warming"] == 3
        assert {item["id"] for item in snapshot["items"] if item["state"] == "warming"} == {
            stale.id,
            second.id,
            third.id,
        }
        assert len({stale.id, second.id, third.id}) == 3
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(started.get(), 0.05)

        pool.configure(4, 1)
        gates[stale.id].set()
        gates[second.id].set()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(started.get(), 0.05)
        assert third.busy and pool.snapshot()["warming"] == 1

        gates[third.id].set()
        fourth = await asyncio.wait_for(started.get(), 1)
        assert fourth.id not in {stale.id, second.id, third.id}
        assert pool.snapshot()["warming"] == 1
    finally:
        loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop
    assert pool.snapshot()["warming"] == 0


async def test_stopping_parallel_warmers_returns_leases_and_removes_unfinished_profiles(
    pool, monkeypatch
):
    leases = [Lease(f"lease-{i}", "203.0.113.1", {"kind": "direct"}) for i in range(2)]
    monkeypatch.setattr(pool._scheduler, "lease", AsyncMock(side_effect=leases))
    release = AsyncMock()
    monkeypatch.setattr(pool._scheduler, "release", release)
    opened = asyncio.Queue()

    @asynccontextmanager
    async def open_profile(profile, route, *, warming):
        profile.mkdir(exist_ok=True)

        async def goto(*args, **kwargs):
            opened.put_nowait(profile)
            await asyncio.Future()

        yield SimpleNamespace(goto=goto)

    monkeypatch.setattr(pool._browsers, "open", open_profile)
    pool.configure(2, 2)
    loop = asyncio.create_task(pool.run())
    try:
        paths = [await asyncio.wait_for(opened.get(), 1) for _ in leases]
        assert len(set(paths)) == 2
        assert all(path.is_dir() for path in paths)
    finally:
        loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop
    assert {call.args[0].id for call in release.await_args_list} == {lease.id for lease in leases}
    assert all(not path.exists() for path in paths)
    assert pool.snapshot()["warming"] == 0


def test_refresh_and_retire_from_the_page(pool):
    pool.target = 1
    ready, busy = add(pool, "ready"), add(pool, "busy", busy=True)
    client = control(pool)

    assert client.post("/api/identities/ready/refresh").status_code == 204
    assert not ready.fresh(time.time())
    assert pool._next() is ready  # 已达到目标时仍能手动刷新
    assert client.post("/api/identities/busy/retire").status_code == 409
    assert client.post("/api/identities/nobody/retire").status_code == 404
    assert client.post("/api/identities/ready/retire").status_code == 204
    assert "ready" not in pool.identities and not pool.profile(ready).exists()
    assert busy.id in pool.identities


@pytest.fixture
def pressure_pool(tmp_path, monkeypatch):
    settings = Settings(service_token=TOKEN, profile_root=tmp_path, crawl_policy_enabled=True)
    pool = IdentityPool(settings, Mock(spec=IdentityStore), Scheduler(settings), Browsers("chrome"))
    pool.target, pool.browser = 3, "146"
    now = [100.0]
    monkeypatch.setattr(
        identities, "time", SimpleNamespace(monotonic=lambda: now[0], time=time.time)
    )
    return pool, now


async def test_empty_list_session_hold_keeps_download_and_respects_manual_refresh(pressure_pool):
    pool, now = pressure_pool
    identity = add(pool, "shared")
    for _ in range(identities.COUNTED_FAILS):
        pool.give_back(identity, "empty_response")
    assert identity.listing_refresh_at == 280
    assert identity.session is not None and identity in pool.available()
    assert pool.available_for_crawl() == []
    with pytest.raises(Wait) as blocked:
        pool.take(for_listing=True)
    assert blocked.value.code == "crawl_identity_hold"
    assert await pool.take_for_download() is identity
    pool.give_back(identity, "ok")
    assert pool.download_credentials(identity.id) == ("ua", "TOKEN-VALUE")
    pool.refresh(identity)
    assert pool.download_credentials(identity.id) == ("ua", "TOKEN-VALUE")
    assert pool._next() is None
    now[0] = 280
    assert pool._next() is identity and pool.available_for_crawl() == []


async def test_pressure_warm_starts_remain_ninety_seconds_apart_despite_wakes(
    pressure_pool,
    monkeypatch,
):
    pool, now = pressure_pool
    stale = [add(pool, name, warmed=False) for name in ("first", "second", "third")]
    for identity in stale:
        identity.listing_fails = identities.COUNTED_FAILS
        identity.listing_refresh_at = now[0]
    grants = AsyncMock(return_value=Lease("warm-lease", "203.0.113.1", {"kind": "direct"}))
    release = AsyncMock()
    monkeypatch.setattr(pool._scheduler, "lease", grants)
    monkeypatch.setattr(pool._scheduler, "release", release)
    starts = asyncio.Queue()
    finish_first = asyncio.Event()

    @asynccontextmanager
    async def open_profile(profile, route, *, warming):
        async def goto(*args, **kwargs):
            starts.put_nowait((profile.name, now[0]))
            if profile.name == "first":
                await finish_first.wait()

        yield SimpleNamespace(goto=goto, wait_for_selector=AsyncMock())

    monkeypatch.setattr(pool._browsers, "open", open_profile)
    monkeypatch.setattr(
        identities,
        "session_of",
        AsyncMock(side_effect=lambda _: Session({"tt_chain_token": "new"}, "ua", "146")),
    )
    pool.configure(3, 3)
    loop = asyncio.create_task(pool.run())
    try:
        assert await asyncio.wait_for(starts.get(), 1) == ("first", 100.0)
        assert grants.await_args.kwargs["warm_bootstrap"] is True
        pool.refresh(stale[1])
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(starts.get(), 0.03)
        finish_first.set()
        now[0] = 189.9
        pool.refresh(stale[1])
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(starts.get(), 0.03)
        grants.assert_awaited_once()
        assert stale[0].listing_refresh_at is None and stale[0] in pool.available_for_crawl()
        now[0] = 190
        pool.refresh(stale[1])
        assert await asyncio.wait_for(starts.get(), 1) == ("second", 190.0)
        assert "warm_bootstrap" not in grants.await_args.kwargs
    finally:
        loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop
    assert release.await_count == 2


async def test_scheduler_warm_wait_gates_wakes_and_releases_the_identity(
    pressure_pool, monkeypatch
):
    pool, now = pressure_pool
    identity = add(pool, "shared", warmed=False, busy=True)
    pool._warming[identity.id] = identity
    wait = Wait(180, "冷却期间等线路", code="warm_empty_wait")
    monkeypatch.setattr(pool._scheduler, "lease", AsyncMock(side_effect=wait))
    await pool._attempt(identity)
    assert not identity.busy and pool._warming == {} and identity.warm_retry_at == 0
    pool.configure(3, 3)
    loop = asyncio.create_task(pool.run())
    try:
        now[0] = 279.9
        pool.refresh(identity)
        await asyncio.sleep(0.03)
        pool._scheduler.lease.assert_awaited_once()
    finally:
        loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop


async def test_profile_protection_keeps_retired_directories_across_restore(pressure_pool):
    pool, _ = pressure_pool
    identity = add(pool, "001122334455")
    pool.retire(identity, "策略测试")
    await pool.persist()
    [retired] = list(pool._store.save.call_args.args[0])
    pool._store.records.return_value = [retired]
    await pool.restore()
    assert identity.id not in pool.identities
    assert pool.profile(identity).is_dir()
