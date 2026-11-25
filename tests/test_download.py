import asyncio
import json
import time
from contextlib import asynccontextmanager
from errno import ENOSPC
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from curl_cffi.requests.exceptions import ConnectionError as CurlConnectionError

from video_crawler import download, tiktok
from video_crawler.browser import Session
from video_crawler.errors import Retry, Wait
from video_crawler.identities import Identity, IdentityPool
from video_crawler.runner import DOWNLOAD_VIDEO
from video_crawler.scheduler import CRAWL, DOWNLOAD, ClaimedTask, Lease
from video_crawler.settings import Settings

WORK = "https://www.tiktok.com/@example/video/7"
CACHED = "https://v16.tiktokcdn.com/cached"
FRESH = "https://v16.tiktokcdn.com/fresh"
RESIDENTIAL = {"kind": "proxy", "protocol": "http", "host": "192.0.2.1", "port": 25300}
DIRECT = {"kind": "direct"}


def page_data(video: dict) -> dict:
    return {"webapp.video-detail": {"itemInfo": {"itemStruct": {"id": "7", "video": video}}}}


def html(data: dict) -> str:
    body = json.dumps({"__DEFAULT_SCOPE__": data})
    return f'<script id="{tiktok.DATA_ID}">{body}</script>'


@pytest.fixture
def attempt(tmp_path, monkeypatch):
    task = ClaimedTask(
        "task",
        1,
        DOWNLOAD_VIDEO,
        "attempt",
        {"batch_id": "b1", "work": {"work_id": "7", "url": WORK}},
    )
    identity = Identity("visitor", session=Session({"tt_chain_token": "test"}, "UA", "1"))
    pool = SimpleNamespace(
        take_for_download=AsyncMock(return_value=identity),
        download_credentials=Mock(
            side_effect=lambda key: ("UA", "test") if key == identity.id else None
        ),
        give_back=Mock(),
        note=Mock(),
        record_download=Mock(),
        profile=Mock(return_value=tmp_path / identity.id),
        refreshed=Mock(),
    )
    trace, leases, outcomes = [], [], {}

    async def grant(use, task_id, avoid, *, attempt_id):
        assert use in {CRAWL, DOWNLOAD}
        assert (task_id, attempt_id, avoid) == (task.task_id, task.attempt_id, task.avoid)
        name, route = ("US", RESIDENTIAL) if use == CRAWL else ("HK", DIRECT)
        trace.append(("lease", name))
        lease = Lease(f"lease-{len(leases)}", name, route)
        leases.append(lease)
        return lease

    async def release(lease, outcome, detail, retry_after):
        trace.append(("release", lease.line, outcome))

    async def upload(task, path, sha256):
        assert path.name == "task-1-attempt.mp4" and sha256 == "digest"
        trace.append(("upload",))
        return 204

    scheduler = SimpleNamespace(
        lease=AsyncMock(side_effect=grant),
        release=AsyncMock(side_effect=release),
        upload=AsyncMock(side_effect=upload),
    )
    curl_response = SimpleNamespace(
        status_code=200, text=html(page_data({"playAddr": FRESH})), headers={}
    )

    async def get_page(url, *, headers, cookies):
        assert (url, headers, cookies) == (WORK, {"User-Agent": "UA"}, identity.session.cookies)
        trace.append(("page", "curl"))
        return curl_response

    curl_get = AsyncMock(side_effect=get_page)

    class CurlSession:
        def __init__(self, *, proxy, trust_env, **kwargs):
            assert proxy == "http://192.0.2.1:25300" and trust_env is False

        async def __aenter__(self):
            return SimpleNamespace(get=curl_get)

        async def __aexit__(self, *args):
            pass

    page = SimpleNamespace(goto=AsyncMock(return_value=None))

    @asynccontextmanager
    async def open_browser(profile, route):
        assert profile == tmp_path / identity.id and route == RESIDENTIAL
        trace.append(("page", "chrome"))
        yield page

    browser_data = AsyncMock(return_value=page_data({"playAddr": FRESH}))

    async def transfer(client, url, path, progress, **options):
        assert client.headers["Referer"] == WORK
        assert client.headers["User-Agent"] == "UA"
        assert client.headers["Cookie"] == "tt_chain_token=test"
        assert client.timeout.read == options["slow_window"] == 45
        assert options["slow_bytes"] == 20 * 1024
        assert (options["range_parts"], options["range_min_bytes"]) == (4, 1024 * 1024)
        trace.append(("fetch", url))
        if url in outcomes:
            raise outcomes[url]
        return "digest"

    monkeypatch.setattr(download, "MIN_FREE", 0)
    monkeypatch.setattr(download, "AsyncSession", CurlSession)
    monkeypatch.setattr(download, "transfer", transfer)
    monkeypatch.setattr(download, "page_data", browser_data)
    monkeypatch.setattr(download, "session_of", AsyncMock(return_value=identity.session))
    browsers = SimpleNamespace(open=open_browser)
    cache = tiktok.MediaCache()
    downloader = download.Downloader(pool, scheduler, browsers, tmp_path, media_cache=cache)
    return SimpleNamespace(
        run=downloader,
        task=task,
        pool=pool,
        scheduler=scheduler,
        identity=identity,
        trace=trace,
        outcomes=outcomes,
        cache=cache,
        leases=leases,
        curl_response=curl_response,
        curl_get=curl_get,
        browser_data=browser_data,
    )


async def test_download_does_not_request_a_line_without_an_identity(attempt):
    attempt.pool.take_for_download.side_effect = Wait(10, "没有可用身份")
    with pytest.raises(Wait, match="没有可用身份"):
        await attempt.run(attempt.task)
    attempt.scheduler.lease.assert_not_awaited()
    assert not attempt.run.progress()


@pytest.mark.parametrize("write_failed", [False, True], ids=["pre-check", "write-failed"])
async def test_disk_full_wait_is_scoped_to_downloads(attempt, monkeypatch, write_failed):
    if write_failed:
        attempt.outcomes[FRESH] = OSError(ENOSPC, "disk full")
    else:
        monkeypatch.setattr(download, "MIN_FREE", 1)
        monkeypatch.setattr(download.shutil, "disk_usage", lambda _: SimpleNamespace(free=0))

    with pytest.raises(Wait, match="磁盘空间不足") as failure:
        await attempt.run(attempt.task)
    assert (failure.value.use, failure.value.seconds) == (DOWNLOAD, 60)
    attempt.scheduler.upload.assert_not_awaited()
    assert not attempt.run.progress()


@pytest.mark.parametrize("http_page_available", [True, False], ids=["http", "chrome"])
async def test_download_fetches_the_preferred_quality_from_either_page_source(
    attempt, http_page_available
):
    hd = "https://v16.tiktokcdn.com/hd"
    video = {
        "playAddr": hd,
        "bitrateInfo": [
            {"GearName": "normal_1080_0", "PlayAddr": {"UrlList": [hd]}},
            {"GearName": "normal_720_0", "PlayAddr": {"UrlList": [FRESH]}},
        ],
    }
    attempt.curl_response.text = html(page_data(video)) if http_page_available else "challenge"
    attempt.browser_data.return_value = page_data(video)
    await attempt.run(attempt.task)
    assert [entry for entry in attempt.trace if entry[0] == "fetch"] == [("fetch", FRESH)]
    attempt.scheduler.upload.assert_awaited_once()


async def test_us_page_lease_is_released_before_hk_media_is_requested(attempt):
    await attempt.run(attempt.task)
    assert attempt.trace == [
        ("lease", "US"),
        ("page", "curl"),
        ("release", "US", "normal"),
        ("lease", "HK"),
        ("fetch", FRESH),
        ("release", "HK", "normal"),
        ("upload",),
    ]
    attempt.pool.give_back.assert_called_once_with(attempt.identity, "neutral")
    attempt.pool.record_download.assert_called_once_with(
        attempt.identity.id, blocked=False, succeeded=True, token="test"
    )


@pytest.mark.parametrize(
    "curl_data", [None, {}, page_data({})], ids=["challenge", "no-work", "no-media"]
)
async def test_chrome_falls_back_with_the_same_identity_and_us_lease(attempt, curl_data):
    attempt.curl_response.text = html(curl_data) if curl_data is not None else "challenge"
    await attempt.run(attempt.task)
    assert attempt.trace[:5] == [
        ("lease", "US"),
        ("page", "curl"),
        ("page", "chrome"),
        ("release", "US", "normal"),
        ("lease", "HK"),
    ]
    attempt.pool.profile.assert_called_once_with(attempt.identity)
    attempt.pool.refreshed.assert_called_once_with(attempt.identity, attempt.identity.session)


async def test_a_page_failure_does_not_request_or_penalize_the_download_line(attempt):
    attempt.curl_response.text = "challenge"
    attempt.browser_data.return_value = None
    with pytest.raises(Retry, match="挑战页") as failure:
        await attempt.run(attempt.task)
    assert failure.value.line == "US"
    assert attempt.trace[-1] == ("release", "US", "suspect")
    assert [lease.line for lease in attempt.leases] == ["US"]
    attempt.pool.give_back.assert_called_once_with(attempt.identity, "neutral")
    attempt.scheduler.upload.assert_not_awaited()


@pytest.mark.parametrize("status", [407, 429, None], ids=["http-407", "http-429", "connect-407"])
async def test_proxy_auth_and_throttling_wait_without_downloading(attempt, status):
    if status is None:
        attempt.curl_get.side_effect = CurlConnectionError("CONNECT tunnel failed, response 407")
    else:
        attempt.curl_response.status_code = status
        attempt.curl_response.headers = {"Retry-After": "30"}
    with pytest.raises(Wait, match=str(status or 407)) as result:
        await attempt.run(attempt.task)
    assert result.value.use == CRAWL
    assert result.value.seconds == (30 if status == 429 else 60)
    assert [lease.line for lease in attempt.leases] == ["US"]
    assert attempt.trace[-1] == ("release", "US", "throttled" if status == 429 else "auth_failed")
    attempt.pool.give_back.assert_called_once_with(attempt.identity, "neutral")


async def test_cache_hit_only_requests_the_download_lease(attempt):
    attempt.cache.remember(attempt.identity.id, "7", [CACHED])
    await attempt.run(attempt.task)
    assert attempt.trace == [
        ("lease", "HK"),
        ("fetch", CACHED),
        ("release", "HK", "normal"),
        ("upload",),
    ]
    attempt.pool.take_for_download.assert_not_awaited()
    attempt.pool.give_back.assert_not_called()


async def test_cache_is_ignored_when_its_original_identity_is_unavailable(attempt):
    attempt.cache.remember("busy", "7", [CACHED])
    await attempt.run(attempt.task)
    assert [lease.line for lease in attempt.leases] == ["US", "HK"]
    assert ("fetch", CACHED) not in attempt.trace


@pytest.mark.parametrize("failure", ["403", "skip"])
async def test_stale_cached_links_retry_via_us_without_penalizing_the_identity(attempt, failure):
    second = f"{CACHED}-2"
    attempt.cache.remember(attempt.identity.id, "7", [CACHED, second])
    if failure == "403":
        attempt.outcomes[CACHED] = download.Stop("403", blocked=True, stale=True)
    else:
        attempt.outcomes.update({url: download.Skip("404") for url in (CACHED, second)})

    await attempt.run(attempt.task)
    assert [lease.line for lease in attempt.leases] == ["HK", "US", "HK"]
    assert [entry for entry in attempt.trace if entry[0] == "release"] == [
        ("release", "HK", "normal"),
        ("release", "US", "normal"),
        ("release", "HK", "normal"),
    ]
    assert (("fetch", second) in attempt.trace) == (failure == "skip")
    assert attempt.cache.get("7") is None
    attempt.pool.note.assert_called_once_with("download", failure != "403")
    attempt.pool.record_download.assert_called_once_with(
        attempt.identity.id, blocked=False, succeeded=True, token="test"
    )
    attempt.pool.give_back.assert_called_once_with(attempt.identity, "neutral")


async def test_a_fresh_403_only_penalizes_the_download_line(attempt):
    attempt.outcomes[FRESH] = download.Stop("403", blocked=True, stale=True)
    with pytest.raises(Retry, match="403") as failure:
        await attempt.run(attempt.task)
    assert failure.value.line == "HK"
    assert ("release", "US", "normal") in attempt.trace
    assert attempt.trace[-1] == ("release", "HK", "suspect")
    attempt.pool.give_back.assert_called_once_with(attempt.identity, "neutral")
    attempt.pool.record_download.assert_called_once_with(
        attempt.identity.id, blocked=True, succeeded=False, token="test"
    )


@pytest.fixture
def live_pool(attempt, tmp_path):
    identity = attempt.identity
    identity.browser, identity.warmed_at = identity.session.browser, time.time()
    pool = IdentityPool(
        Settings(service_token="test", profile_root=tmp_path),
        Mock(),
        attempt.scheduler,
        attempt.run._browsers,
    )
    pool.browser = identity.browser
    pool.identities[identity.id] = identity
    attempt.run._pool = pool
    return pool


@pytest.mark.parametrize("cache_hit", [False, True], ids=["fresh-link", "cached-while-busy"])
async def test_download_uses_a_snapshot_while_another_task_holds_the_identity(
    attempt, live_pool, cache_hit
):
    got_credentials, may_fetch = asyncio.Event(), asyncio.Event()
    grant = attempt.scheduler.lease.side_effect
    release = attempt.scheduler.release.side_effect

    async def lease(use, *args, **kwargs):
        if use == DOWNLOAD and cache_hit:
            got_credentials.set()
            await may_fetch.wait()
        return await grant(use, *args, **kwargs)

    async def release_line(line, *args, **kwargs):
        if line.line == "US":
            got_credentials.set()
            await may_fetch.wait()
        return await release(line, *args, **kwargs)

    attempt.scheduler.lease.side_effect = lease
    attempt.scheduler.release.side_effect = release_line
    if cache_hit:
        attempt.cache.remember(attempt.identity.id, "7", [CACHED])
        borrowed = live_pool.take()
        assert attempt.run.has_cached_media() and attempt.run.has_cached_media("7")

    work = asyncio.create_task(attempt.run(attempt.task))
    try:
        await asyncio.wait_for(got_credentials.wait(), 1)
        if not cache_hit:
            borrowed = live_pool.take()
        assert borrowed is attempt.identity and borrowed.busy
        live_pool.refreshed(borrowed, Session({"tt_chain_token": "new"}, "new-UA", "1"))
        # The transfer fixture checks that the headers still use the original credentials.
        may_fetch.set()
        await asyncio.wait_for(work, 1)
        assert borrowed.busy
        assert live_pool.download_credentials(borrowed.id) == ("new-UA", "new")
        assert [lease.line for lease in attempt.leases] == (["HK"] if cache_hit else ["US", "HK"])
    finally:
        work.cancel()
        await asyncio.gather(work, return_exceptions=True)
        live_pool.give_back(attempt.identity)


async def test_fresh_403_cools_the_original_identity_during_concurrent_crawl(
    attempt, live_pool, monkeypatch
):
    transfer = download.transfer

    async def refused(*args, **kwargs):
        assert live_pool.take(preferred_id=attempt.identity.id) is attempt.identity
        return await transfer(*args, **kwargs)

    original_session = attempt.identity.session
    other = Identity("other", browser="1", warmed_at=time.time(), last_used_at=time.time())
    other.session = Session({"tt_chain_token": "other"}, "other-UA", "1")
    live_pool.identities[other.id] = other
    monkeypatch.setattr(download, "transfer", refused)
    attempt.outcomes[FRESH] = download.Stop("403", blocked=True, stale=True)

    try:
        with pytest.raises(Retry, match="403"):
            await attempt.run(attempt.task)
        assert attempt.identity.busy
        assert attempt.identity.fails == 1 and attempt.identity.session is None
        assert attempt.identity.cooldown_until > time.time()
        assert live_pool.available() == [other]
        snapshot = live_pool.snapshot()
        assert (snapshot["download_total"], snapshot["download_blocked"]) == (1, 1)
        live_pool.refreshed(attempt.identity, original_session)
    finally:
        live_pool.give_back(attempt.identity, "ok")
    assert attempt.identity.fails == 1 and attempt.identity.session is None


async def test_windows_cleanup_error_keeps_the_original_failure(attempt, caplog, monkeypatch):
    attempt.curl_response.status_code = 407

    def locked_file(self, *, missing_ok):
        assert missing_ok and self.name == "task-1-attempt.mp4"
        raise PermissionError("file is in use")

    monkeypatch.setattr(Path, "unlink", locked_file)
    with pytest.raises(Wait, match="407"):
        await attempt.run(attempt.task)
    assert "无法清理下载文件" in caplog.text
    assert not attempt.run.progress()
