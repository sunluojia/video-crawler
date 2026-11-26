import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest

from video_crawler import crawl, tiktok
from video_crawler.api import control_api
from video_crawler.browser import Session
from video_crawler.errors import Retry, Wait
from video_crawler.identities import Identity
from video_crawler.runner import Runner
from video_crawler.scheduler import CRAWL, ClaimedTask, Lease


async def test_profile_and_listing_progress_use_the_same_browser_and_keep_early_responses(
    tmp_path, monkeypatch
):
    cutoff = 1_730_000_000
    task = ClaimedTask(
        str(uuid4()),
        1,
        "crawl_creator",
        str(uuid4()),
        {
            "batch_id": str(uuid4()),
            "creator": {"handle": "alice"},
            "cutoff": datetime.fromtimestamp(cutoff, UTC).isoformat(),
            "max_works": 5,
        },
    )
    lease = Lease("lease", "direct:test", {"kind": "direct"})
    identity = Identity("visitor")
    pool = SimpleNamespace(
        take=Mock(return_value=identity),
        profile=Mock(return_value=tmp_path / "visitor"),
        note=Mock(),
        refreshed=Mock(),
        give_back=Mock(),
    )
    scheduler = SimpleNamespace(lease=AsyncMock(return_value=lease), release=AsyncMock())
    session = Session({"tt_chain_token": "test-token"}, "test-agent", "1")
    monkeypatch.setattr(crawl, "session_of", AsyncMock(return_value=session))
    profile = {"webapp.user-detail": {"userInfo": {"user": {"uniqueId": "alice"}}}}
    monkeypatch.setattr(crawl, "page_data", AsyncMock(return_value=profile))

    def item(work_id, seconds, **fields):
        return {
            "id": work_id,
            "createTime": cutoff + seconds,
            "author": {"uniqueId": "alice"},
            **fields,
        }

    handler = Mock()
    profile_opened, listing_waiting = asyncio.Event(), asyncio.Event()
    finish_profile, finish_listing = asyncio.Event(), asyncio.Event()

    async def respond(cursor, next_cursor, items, *, more):
        data = {"cursor": next_cursor, "itemList": items, "hasMore": more}
        await handler.call_args_list[0].args[1](
            SimpleNamespace(
                url=f"https://www.tiktok.com/api/post/item_list/?cursor={cursor}",
                status=200,
                headers={},
                body=AsyncMock(return_value=json.dumps(data).encode()),
            )
        )

    media_url = "https://v16.tiktokcdn.com/a?signature=test"
    primary = "https://v16.tiktokcdn.com/720"
    video = {
        "playAddr": primary,
        "bitrateInfo": [
            {"GearName": "normal_720_0", "PlayAddr": {"UrlList": [primary]}},
            {"GearName": "normal_540_0", "PlayAddr": {"UrlList": [media_url]}},
        ],
    }

    async def goto(*args, **kwargs):
        # 首屏作品页可能先于达人资料到达。
        await respond("0", "1", [item("7", -1, video=video), item("8", 1)], more=True)
        profile_opened.set()
        await finish_profile.wait()

    async def wheel(*args):
        listing_waiting.set()
        await finish_listing.wait()
        await respond(
            "1", "2", [item("7", -1), item("6", -2, imagePost={"images": []})], more=False
        )

    page = SimpleNamespace(
        on=handler, remove_listener=Mock(), goto=goto, mouse=SimpleNamespace(wheel=wheel)
    )

    @asynccontextmanager
    async def open_browser(profile, route, *, line):
        yield page

    browsers = SimpleNamespace(open_crawl=Mock(side_effect=open_browser))
    media_cache = tiktok.MediaCache()
    crawler = crawl.Crawler(
        pool, scheduler, browsers, media_cache=media_cache, max_video_height=540
    )
    token = "t" * 16
    runner = Runner(scheduler, {}, {}, available_identities=lambda: 0, publish=AsyncMock())
    app = control_api(token, runner, pool, scheduler, crawls=crawler.progress)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app),
        base_url="http://crawler",
        headers={"Authorization": f"Bearer {token}"},
    ) as client:
        job = asyncio.create_task(crawler(task))
        try:
            await asyncio.wait_for(profile_opened.wait(), 1)
            [progress] = (await client.get("/api/crawl-progress")).json()
            assert progress == {
                "task_id": task.task_id,
                "handle": "alice",
                "stage": "profile",
                "pages_read": 0,
                "works_found": 0,
                "max_works": 5,
            }

            finish_profile.set()
            await asyncio.wait_for(listing_waiting.wait(), 1)
            [progress] = (await client.get("/api/crawl-progress")).json()
            assert (progress["stage"], progress["pages_read"], progress["works_found"]) == (
                "listing",
                1,
                1,
            )

            finish_listing.set()
            result = await asyncio.wait_for(job, 1)
            assert result["conclusion"] == "ok"
            assert [work["work_id"] for work in result["works"]] == ["7", "6"]
            cached = media_cache.get("7")
            assert cached is not None and (cached.identity_id, cached.urls) == (
                identity.id,
                (media_url,),
            )
            assert media_url not in json.dumps(result)
            assert (await client.get("/api/crawl-progress")).json() == []
        finally:
            job.cancel()
            await asyncio.gather(job, return_exceptions=True)

    browsers.open_crawl.assert_called_once_with(
        pool.profile(identity), lease.route, line=lease.line
    )
    assert page.remove_listener.call_args_list == handler.call_args_list
    scheduler.lease.assert_awaited_once_with(CRAWL, task.task_id, [], attempt_id=task.attempt_id)
    scheduler.release.assert_awaited_once_with(
        lease, "normal", None, None, listing_result="complete"
    )
    pool.refreshed.assert_called_once_with(identity, session)
    pool.give_back.assert_called_once_with(identity, "listing_ok")


@pytest.mark.parametrize("conclusion", ["private", "not_found"])
async def test_listing_is_skipped_for_private_or_missing_creators(monkeypatch, conclusion):
    progress = crawl.CrawlProgress("task", "alice", 5, stage="profile")
    monkeypatch.setattr(crawl, "_read_profile", AsyncMock(return_value=(conclusion, None)))
    page = SimpleNamespace(on=Mock(), remove_listener=Mock())
    assert await crawl._read(page, {}, "direct:test", Mock(), progress, identity_id="visitor") == {
        "conclusion": conclusion,
        "profile": None,
    }
    assert progress.stage == "profile"


@pytest.fixture
def listing_attempt(monkeypatch):
    task = ClaimedTask(
        "task",
        1,
        "crawl_creator",
        "attempt",
        {"creator": {"handle": "alice"}, "cutoff": "2026-11-20T00:00:00Z", "max_works": 5},
    )
    lease = Lease("lease", "proxy:test", {"kind": "proxy"})
    scheduler = SimpleNamespace(lease=AsyncMock(return_value=lease), release=AsyncMock())
    identity = Identity("abc123def456")
    pool = SimpleNamespace(
        take=Mock(return_value=identity), profile=Mock(), note=Mock(), give_back=Mock()
    )
    handlers = {}
    page = SimpleNamespace(
        on=lambda event, callback: handlers.update({event: callback}),
        remove_listener=lambda event, _: handlers.pop(event),
        reload=AsyncMock(),
        mouse=SimpleNamespace(wheel=AsyncMock()),
        evaluate=AsyncMock(return_value="Videos Reposts Liked"),
    )

    @asynccontextmanager
    async def open_browser(profile, route, *, line):
        yield page

    browsers = SimpleNamespace(open_crawl=open_browser)
    monkeypatch.setattr(crawl, "session_of", AsyncMock(return_value=None))
    crawler = crawl.Crawler(pool, scheduler, browsers)

    def with_profile(side_effect):
        monkeypatch.setattr(crawl, "_read_profile", AsyncMock(side_effect=side_effect))

    return SimpleNamespace(
        task=task,
        lease=lease,
        release=scheduler.release,
        page=page,
        handlers=handlers,
        note=pool.note,
        identity=identity,
        give_back=pool.give_back,
        with_profile=with_profile,
        run=crawler,
    )


async def test_crawl_removes_listeners_and_cancels_in_flight_body_reads(listing_attempt):
    attempt = listing_attempt
    started = asyncio.Event()

    async def read_body():
        started.set()
        await asyncio.Future()

    response = SimpleNamespace(
        url="https://www.tiktok.com/api/post/item_list/?cursor=0",
        status=200,
        body=AsyncMock(side_effect=read_body),
    )
    callbacks, jobs = [], []

    async def read_profile(*args, **kwargs):
        callbacks.append(attempt.handlers["response"])
        jobs.append(asyncio.create_task(callbacks[0](response)))
        await asyncio.wait_for(started.wait(), 1)
        return "private", None

    attempt.with_profile(read_profile)
    assert (await attempt.run(attempt.task))["conclusion"] == "private"
    assert jobs[0].cancelled() and not attempt.handlers
    await callbacks[0](response)  # 已排队但还未执行的监听器也必须停止。
    response.body.assert_awaited_once()
    attempt.note.assert_not_called()


@pytest.mark.parametrize(
    ("status", "body", "result", "detail", "reloads"),
    [
        (200, b'{"statusCode": 0, "hasMore": false}', "complete", "无符合条件的作品", 0),
        (200, b"", "empty_response", "两次返回空正文", 1),
        (200, crawl.BrowserError("body unavailable"), "read_failed", "正文读取失败", 0),
        (200, b'{"statusCode": 1, "hasMore": false}', "read_failed", "列表格式不对", 0),
        (200, b"{}", "read_failed", "列表格式不对", 0),
        (503, b"", "read_failed", "HTTP 503", 0),
        (429, b"", None, "限流（429）", 0),
        (407, b"", None, "代理认证失败（407）", 0),
    ],
)
async def test_listing_reports_distinct_response_failures(
    listing_attempt, caplog, status, body, result, detail, reloads
):
    attempt = listing_attempt
    caplog.set_level("INFO", logger=crawl.__name__)
    read = (
        AsyncMock(side_effect=body) if isinstance(body, Exception) else AsyncMock(return_value=body)
    )

    async def respond(*_, **__):
        await attempt.handlers["response"](
            SimpleNamespace(
                url="https://www.tiktok.com/api/post/item_list/?cursor=0&sig=url-marker",
                status=status,
                headers={"retry-after": "120", "set-cookie": "cookie-marker"},
                body=read,
            )
        )
        return None, None

    attempt.with_profile(respond)

    # 模拟浏览器导航方法只返回导航响应，列表响应通过事件送达。
    async def reload_page(**_):
        await respond()

    attempt.page.reload.side_effect = reload_page
    if result == "complete":
        assert (await attempt.run(attempt.task))["works"] == []
        outcome, seconds = "normal", None
    elif status in (429, 407):
        with pytest.raises(Wait, match=detail) as caught:
            await attempt.run(attempt.task)
        outcome, seconds = ("throttled", 120) if status == 429 else ("auth_failed", None)
        assert caught.value.seconds == (seconds or 60)
        read.assert_not_awaited()
    else:
        with pytest.raises(Retry, match=detail):
            await attempt.run(attempt.task)
        outcome, seconds = "suspect", None

    args, fields = attempt.release.await_args
    assert args[:2] == (attempt.lease, outcome)
    assert detail in args[2] and args[3] == seconds
    assert fields["listing_result"] == result
    assert attempt.page.reload.await_count == reloads
    if isinstance(body, Exception):
        attempt.note.assert_not_called()
    returned_as = {"complete": "listing_ok", "empty_response": "empty_response"}
    attempt.give_back.assert_called_once_with(attempt.identity, returned_as.get(result, "neutral"))

    responses = [
        json.loads(message.removeprefix("listing_response "))
        for message in caplog.messages
        if message.startswith("listing_response ")
    ]
    assert responses == [
        {
            "identity_id": attempt.identity.id,
            "line": attempt.lease.line,
            "status": status,
            "content_bytes": len(body) if status == 200 and isinstance(body, bytes) else None,
        }
    ] * (1 + reloads)
    assert "url-marker" not in caplog.text
    assert "cookie-marker" not in caplog.text
    assert "statusCode" not in caplog.text


@pytest.mark.parametrize("second_body", [b'{"statusCode": 0, "hasMore": false}', b""])
async def test_empty_listing_retry_can_recover_or_reveal_login_requirement(
    listing_attempt, second_body
):
    attempt = listing_attempt
    attempt.page.evaluate.return_value = "This creator turned on\naudience controls"

    async def emit(body):
        await attempt.handlers["response"](
            SimpleNamespace(
                url="https://www.tiktok.com/api/post/item_list/?cursor=0",
                status=200,
                body=AsyncMock(return_value=body),
            )
        )

    async def profile(*_):
        await emit(b"")
        return None, None

    async def reload_page(**_):
        await emit(second_body)

    attempt.with_profile(profile)
    attempt.page.reload.side_effect = reload_page
    result = await attempt.run(attempt.task)

    if second_body:
        assert result["conclusion"] == "ok"
        assert attempt.release.await_args.kwargs["listing_result"] == "complete"
        attempt.give_back.assert_called_once_with(attempt.identity, "listing_ok")
    else:
        assert result["conclusion"] == "login_required"
        assert attempt.release.await_args.kwargs["listing_result"] is None
        attempt.give_back.assert_called_once_with(attempt.identity, "neutral")
    assert attempt.page.reload.await_count == 1


@pytest.mark.parametrize("reachable", [True, False])
async def test_a_listing_proxy_error_requires_a_failed_connection_check(
    listing_attempt, monkeypatch, reachable
):
    attempt = listing_attempt
    probe = AsyncMock(return_value=reachable)
    monkeypatch.setattr(crawl, "proxy_reachable", probe)

    async def profile(*_):
        attempt.handlers["requestfailed"](
            SimpleNamespace(
                url="https://www.tiktok.com/api/post/item_list/",
                failure="net::ERR_TUNNEL_CONNECTION_FAILED",
            )
        )
        return None, None

    attempt.with_profile(profile)
    with pytest.raises(Retry if reachable else Wait):
        await attempt.run(attempt.task)
    probe.assert_awaited_once_with(attempt.lease.route)
    assert attempt.release.await_args.args[1] == ("suspect" if reachable else "proxy_down")
    assert attempt.release.await_args.kwargs["listing_result"] == (
        "read_failed" if reachable else None
    )


async def test_an_idle_listing_returns_its_lease_without_reloading_the_profile(
    listing_attempt, monkeypatch
):
    attempt = listing_attempt
    monkeypatch.setattr(crawl, "WAIT_PAGE", 0.01)

    async def profile(*_):
        return None, None

    attempt.with_profile(profile)
    with pytest.raises(Retry, match="等待响应超时"):
        await attempt.run(attempt.task)
    attempt.page.mouse.wheel.assert_awaited_once()
    attempt.page.reload.assert_not_awaited()
    assert attempt.release.await_args.kwargs["listing_result"] == "read_failed"


@pytest.mark.parametrize("notice", [None, "This creator turned on\naudience controls"])
async def test_an_unreadable_listing_is_login_required_or_keeps_the_profile(
    listing_attempt, monkeypatch, notice
):
    """开了受众控制的达人直接得出结论；其他读不到列表的情况带着资料重试。"""
    attempt = listing_attempt
    monkeypatch.setattr(crawl, "WAIT_PAGE", 0.01)
    found = {"handle": "alice", "url": "https://www.tiktok.com/@alice"}
    attempt.with_profile(AsyncMock(return_value=(None, found)))
    if notice:
        attempt.page.evaluate.return_value = notice
        result = await attempt.run(attempt.task)
        assert result == {"conclusion": "login_required", "profile": found}
        assert attempt.release.await_args.args[1] == "normal"
    else:
        with pytest.raises(Retry) as caught:
            await attempt.run(attempt.task)
        assert caught.value.profile == found


def test_retry_after_accepts_seconds_http_dates_and_missing_headers():
    assert crawl.retry_after_seconds("7200") == 7200
    date = format_datetime(datetime.now(UTC) + timedelta(seconds=120), usegmt=True)
    assert 118 <= crawl.retry_after_seconds(date) <= 120
    assert crawl.retry_after_seconds("later") == 60
    assert crawl.retry_after_seconds(None) == 60
