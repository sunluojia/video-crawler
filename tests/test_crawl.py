import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest

from video_crawler import crawl
from video_crawler.api import control_api
from video_crawler.browser import Session
from video_crawler.identities import Identity
from video_crawler.runner import Runner
from video_crawler.scheduler import CRAWL, Lease
from video_crawler.store import Store, Task


async def test_profile_and_listing_progress_use_the_same_browser_and_keep_early_responses(
    tmp_path, monkeypatch
):
    cutoff = 1_730_000_000
    task = Task(
        str(uuid4()),
        1,
        "crawl_creator",
        str(uuid4()),
        {
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
        await handler.call_args.args[1](
            SimpleNamespace(
                url=f"https://www.tiktok.com/api/post/item_list/?cursor={cursor}",
                body=AsyncMock(return_value=json.dumps(data).encode()),
            )
        )

    async def goto(*args, **kwargs):
        # 首屏作品页可能先于达人资料到达。
        await respond("0", "1", [item("7", -1), item("8", 1)], more=True)
        profile_opened.set()
        await finish_profile.wait()

    async def wheel(*args):
        listing_waiting.set()
        await finish_listing.wait()
        await respond(
            "1", "2", [item("7", -1), item("6", -2, imagePost={"images": []})], more=False
        )

    page = SimpleNamespace(on=handler, goto=goto, mouse=SimpleNamespace(wheel=wheel))

    @asynccontextmanager
    async def open_browser(profile, route):
        yield page

    browsers = SimpleNamespace(open=Mock(side_effect=open_browser))
    crawler = crawl.Crawler(pool, scheduler, browsers)
    store = Store(tmp_path / "state.db")
    token = "t" * 16
    app = control_api(token, store, Runner(store, {}, {}), pool, scheduler, crawls=crawler.progress)
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
            assert (await client.get("/api/crawl-progress")).json() == []
        finally:
            job.cancel()
            await asyncio.gather(job, return_exceptions=True)

    browsers.open.assert_called_once()
    scheduler.lease.assert_awaited_once_with(CRAWL, task.task_id, [])
    scheduler.release.assert_awaited_once_with(lease, "normal", None)
    pool.refreshed.assert_called_once_with(identity, session)
    pool.give_back.assert_called_once_with(identity, "ok")


@pytest.mark.parametrize("conclusion", ["private", "not_found"])
async def test_listing_is_skipped_for_private_or_missing_creators(monkeypatch, conclusion):
    progress = crawl.CrawlProgress("task", "alice", 5, stage="profile")
    monkeypatch.setattr(crawl, "_read_profile", AsyncMock(return_value=(conclusion, None)))
    page = SimpleNamespace(on=Mock())
    assert await crawl._read(page, {}, "direct:test", Mock(), progress) == {
        "conclusion": conclusion,
        "profile": None,
    }
    assert progress.stage == "profile"
