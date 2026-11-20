"""获取达人资料和最新作品（crawl_creator），见爬取方案第 4 节。

用身份的浏览器打开达人主页：资料从页面数据里读，作品列表抓页面自己发出的请求，
要下一页就滚动，让页面自己去加载。
"""

import asyncio
import json
from collections.abc import Callable
from datetime import datetime

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


class Crawler:
    def __init__(self, pool: IdentityPool, scheduler: Scheduler, browsers: Browsers) -> None:
        self._pool, self._scheduler, self._browsers = pool, scheduler, browsers

    async def __call__(self, task: Task) -> dict:
        """一次尝试，返回 crawled 事件的内容。"""
        identity = self._pool.take()
        try:
            lease = await self._scheduler.lease(CRAWL, task.task_id, task.avoid)
        except BaseException:
            self._pool.give_back(identity)
            raise
        outcome, detail, session = "suspect", None, None
        try:
            async with self._browsers.open(self._pool.profile(identity), lease.route) as page:
                result = await _read(page, task.body, lease.line, self._pool.note)
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
            await self._scheduler.release(lease, outcome, detail)
            if session:
                self._pool.refreshed(identity, session)
            self._pool.give_back(identity, "ok" if outcome == "normal" else "neutral")


async def _read(page: Page, body: dict, line: str, note: Callable[[str, bool], None]) -> dict:
    handle = body["creator"]["handle"]
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
    await page.goto(f"https://www.tiktok.com/@{handle}", wait_until="domcontentloaded")
    data = await page_data(page)
    if data is None:
        raise Retry("主页没有页面数据，可能停在了挑战页", line)
    try:
        conclusion, profile = tiktok.profile_of(data, handle)
    except ValueError as error:
        raise Retry(str(error), line) from error
    if conclusion:
        return {"conclusion": conclusion, "profile": profile}

    listing = tiktok.Listing(datetime.fromisoformat(body["cutoff"]), body["max_works"])
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
                    listing.add(cursor, json.loads(content))
                except (ValueError, KeyError, TypeError) as error:
                    raise Retry(f"作品列表格式不对：{error!r}", line) from error
                read += 1
                if not listing.settled:
                    if read >= MAX_PAGES:
                        raise Retry(f"读了 {MAX_PAGES} 页还不能确定前 {listing.limit} 条", line)
                    await page.mouse.wheel(0, SCROLL)
    except TimeoutError:
        raise Retry(f"{LIST_SECONDS} 秒内没能确定前 {listing.limit} 条", line) from None
    return {"conclusion": "ok", "profile": profile, "works": listing.works()}
