"""获取达人资料和最新作品（crawl_creator），见爬取方案第 4 节。

用身份的浏览器打开达人主页：资料从页面数据里读，作品列表抓页面自己发出的请求，
要下一页就滚动，让页面自己去加载。
"""

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from patchright.async_api import Error as BrowserError
from patchright.async_api import Page, Response

from video_crawler import tiktok
from video_crawler.browser import Browsers, page_data, session_of
from video_crawler.errors import Retry
from video_crawler.identities import IdentityPool
from video_crawler.scheduler import CRAWL, Scheduler
from video_crawler.store import Task

LIST_SECONDS = 60
MAX_PAGES = 10
MAX_EMPTY = 7
"""空内容最多 7 次请求：实测最坏的一次前 6 次都是空的，第 7 次才拿到。"""
WAIT_PAGE = 5
SCROLL = 2400


@dataclass
class CrawlProgress:
    task_id: str
    handle: str
    max_works: int
    stage: Literal["waiting_line", "profile", "listing"] = "waiting_line"
    pages_read: int = 0
    works_found: int = 0


class Crawler:
    def __init__(self, pool: IdentityPool, scheduler: Scheduler, browsers: Browsers) -> None:
        self._pool, self._scheduler, self._browsers = pool, scheduler, browsers
        self._active: dict[str, CrawlProgress] = {}

    def progress(self) -> list[CrawlProgress]:
        return list(self._active.values())

    async def __call__(self, task: Task) -> dict:
        """一次尝试，返回 crawled 事件的内容。"""
        progress = CrawlProgress(
            task.task_id, task.body["creator"]["handle"], task.body["max_works"]
        )
        self._active[task.task_id] = progress
        try:
            return await self._collect(task, progress)
        finally:
            self._active.pop(task.task_id, None)

    async def _collect(self, task: Task, progress: CrawlProgress) -> dict:
        identity = self._pool.take()
        try:
            lease = await self._scheduler.lease(CRAWL, task.task_id, task.avoid)
        except BaseException:
            self._pool.give_back(identity)
            raise
        progress.stage = "profile"
        outcome, detail, session = "suspect", None, None
        try:
            async with self._browsers.open(self._pool.profile(identity), lease.route) as page:
                result = await _read(page, task.body, lease.line, self._pool.note, progress)
                session = await session_of(page)
            outcome = "normal"
            return result
        except Retry as error:
            detail = error.reason
            raise
        except BrowserError as error:
            detail = f"浏览器出错：{error}"
            raise Retry(detail, lease.line) from error
        finally:
            try:
                await self._scheduler.release(lease, outcome, detail)
            finally:
                if session:
                    self._pool.refreshed(identity, session)
                self._pool.give_back(identity, "ok" if outcome == "normal" else "neutral")


async def _read(
    page: Page, body: dict, line: str, note: Callable[[str, bool], None], progress: CrawlProgress
) -> dict:
    pages: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()

    async def on_response(response: Response) -> None:
        if tiktok.ITEM_LIST in response.url:
            try:
                content = await response.body()
            except BrowserError:
                content = b""
            note("list", bool(content))
            pages.put_nowait((tiktok.request_cursor(response.url), content))

    page.on("response", on_response)  # 先开始监听，再打开主页
    conclusion, profile = await _read_profile(page, progress.handle, line)
    if conclusion:
        return {"conclusion": conclusion, "profile": profile}

    listing = tiktok.Listing(datetime.fromisoformat(body["cutoff"]), progress.max_works)
    works = await _read_listing(page, pages, listing, line, progress)
    return {"conclusion": "ok", "profile": profile, "works": works}


async def _read_profile(page: Page, handle: str, line: str) -> tuple[str | None, dict | None]:
    await page.goto(f"https://www.tiktok.com/@{handle}", wait_until="domcontentloaded")
    data = await page_data(page)
    if data is None:
        raise Retry("主页没有页面数据，可能停在了挑战页", line)
    try:
        return tiktok.profile_of(data, handle)
    except ValueError as error:
        raise Retry(str(error), line) from error


async def _read_listing(
    page: Page,
    pages: asyncio.Queue[tuple[str, bytes]],
    listing: tiktok.Listing,
    line: str,
    progress: CrawlProgress,
) -> list[dict]:
    progress.stage = "listing"
    empty = read = 0
    try:
        async with asyncio.timeout(LIST_SECONDS):
            while not listing.settled:
                try:
                    cursor, content = await asyncio.wait_for(pages.get(), WAIT_PAGE)
                except TimeoutError:
                    await page.mouse.wheel(0, SCROLL)  # 页面没动静：滚一下让它加载
                    continue
                if not content:
                    empty += 1
                    if empty >= MAX_EMPTY:
                        raise Retry(f"作品列表 {MAX_EMPTY} 次请求都是空内容", line)
                    if listing.started:
                        await page.mouse.wheel(0, SCROLL)
                    else:  # 第一页就是空的：刷新重来
                        await page.reload(wait_until="domcontentloaded")
                    continue
                try:
                    if listing.add(cursor, json.loads(content)):
                        progress.pages_read += 1
                        progress.works_found = listing.candidate_count
                except (ValueError, KeyError, TypeError) as error:
                    raise Retry(f"作品列表格式不对：{error!r}", line) from error
                read += 1
                if not listing.settled:
                    if read >= MAX_PAGES:
                        raise Retry(f"读了 {MAX_PAGES} 页还不能确定前 {listing.limit} 条", line)
                    await page.mouse.wheel(0, SCROLL)
    except TimeoutError:
        raise Retry(f"{LIST_SECONDS} 秒内没能确定前 {listing.limit} 条", line) from None
    return listing.works()
