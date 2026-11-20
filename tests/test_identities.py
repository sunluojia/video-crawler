"""身份池的快照和账号池页面的操作，见爬取方案第 6.3 节。"""

import time

import pytest
from fastapi.testclient import TestClient

from video_crawler.api import control_api
from video_crawler.browser import Browsers, Session
from video_crawler.identities import Identity, IdentityPool
from video_crawler.runner import Runner
from video_crawler.scheduler import Scheduler
from video_crawler.settings import Settings
from video_crawler.store import Store

TOKEN = "t" * 16


@pytest.fixture
def pool(tmp_path) -> IdentityPool:
    """调度中心分给本机 3 个。"""
    settings = Settings(service_token=TOKEN, profile_root=tmp_path)
    store = Store(tmp_path / "state.db")
    pool = IdentityPool(settings, store, Scheduler(settings), Browsers("chrome"))
    pool.target = 3
    return pool


def control(pool: IdentityPool) -> TestClient:
    store = pool._store
    app = control_api(TOKEN, store, Runner(store, {}, {}), pool, pool._scheduler)
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
    add(pool, "a", fails=3)
    assert pool._next() is not None  # 连续失败的不计入，补新的
    for name in "bcd":
        add(pool, name, fails=3)
    assert pool._next() is None  # 最多保留目标的两倍


def test_refresh_and_retire_from_the_page(pool):
    ready, busy = add(pool, "ready"), add(pool, "busy", busy=True)
    client = control(pool)

    assert client.post("/api/identities/ready/refresh").status_code == 204
    assert not ready.fresh(time.time())  # 后台循环下一个就预热它
    assert client.post("/api/identities/busy/retire").status_code == 409
    assert client.post("/api/identities/nobody/retire").status_code == 404
    assert client.post("/api/identities/ready/retire").status_code == 204
    assert "ready" not in pool.identities and not pool.profile(ready).exists()
    assert busy.id in pool.identities
