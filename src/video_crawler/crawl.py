"""获取达人资料和最新作品（crawl_creator），见爬取方案第 4 节。

用身份的浏览器打开达人主页：资料从页面数据里读，作品列表抓页面自己发出的请求，
要下一页就滚动，让页面自己去加载。
"""

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from math import ceil
from typing import Literal

from patchright.async_api import Error as BrowserError
from patchright.async_api import Page, Request, Response

from video_crawler import tiktok
from video_crawler.browser import Browsers, page_data, session_of
from video_crawler.errors import Retry, Wait
from video_crawler.identities import IdentityPool
from video_crawler.routes import proxy_reachable
from video_crawler.scheduler import CRAWL, MAX_RETRY_AFTER, ListingResult, Scheduler
from video_crawler.store import Task

LIST_SECONDS = 30
MAX_PAGES = 10
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


class ListingFailure(Retry):
    def __init__(
        self,
        reason: str,
        line: str,
        result: Literal["empty_response", "read_failed"] = "read_failed",
    ) -> None:
        super().__init__(reason, line)
        self.result = result


class _CrawlWait(Wait):
    def __init__(self, seconds: int, reason: str, outcome: Literal["throttled", "auth_failed"]):
        super().__init__(seconds, reason, CRAWL)
        self.outcome = outcome


type ListingPage = tuple[str, bytes] | ListingFailure | _CrawlWait


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
        listing_result: ListingResult | None = None
        retry_after = None
        try:
            async with self._browsers.open(self._pool.profile(identity), lease.route) as page:
                result = await _read(page, task.body, lease.line, self._pool.note, progress)
                session = await session_of(page)
            outcome = "normal"
            if result["conclusion"] == "ok":
                listing_result = "complete"
                if not result["works"]:
                    detail = "作品列表完整，无符合条件的作品"
            return result
        except _CrawlWait as wait:
            outcome, detail = wait.outcome, wait.reason
            if outcome == "throttled":
                retry_after = int(wait.seconds)
            raise
        except (Retry, BrowserError) as error:
            detail = error.reason if isinstance(error, Retry) else f"浏览器出错：{error}"
            if _proxy_connection_error(detail) and not await proxy_reachable(lease.route):
                outcome, detail = "proxy_down", "连接代理失败，复查代理端口无响应"
                raise Wait(60, detail, CRAWL) from error
            if isinstance(error, ListingFailure):
                listing_result = error.result
            if isinstance(error, Retry):
                raise
            raise Retry(detail, lease.line) from error
        finally:
            if outcome == "suspect" and progress.stage == "listing" and listing_result is None:
                listing_result = "read_failed"
                detail = detail or "作品列表读取中止"
            try:
                await self._scheduler.release(
                    lease, outcome, detail, retry_after, listing_result=listing_result
                )
            finally:
                if session:
                    self._pool.refreshed(identity, session)
                self._pool.give_back(identity, "ok" if outcome == "normal" else "neutral")


async def _read(
    page: Page, body: dict, line: str, note: Callable[[str, bool], None], progress: CrawlProgress
) -> dict:
    pages: asyncio.Queue[ListingPage] = asyncio.Queue()

    async def on_response(response: Response) -> None:
        if tiktok.ITEM_LIST not in response.url:
            return
        if wait := _http_wait(response, "作品列表"):
            pages.put_nowait(wait)
            return
        if response.status != 200:
            pages.put_nowait(ListingFailure(f"作品列表 HTTP {response.status}", line))
            return
        try:
            async with asyncio.timeout(WAIT_PAGE):
                content = await response.body()
        except BrowserError:
            pages.put_nowait(ListingFailure("作品列表正文读取失败", line))
        except TimeoutError:
            pages.put_nowait(ListingFailure("作品列表正文读取超时", line))
        else:
            note("list", bool(content))
            pages.put_nowait((tiktok.request_cursor(response.url), content))

    def on_request_failed(request: Request) -> None:
        failure = request.failure or ""
        if tiktok.ITEM_LIST in request.url and _proxy_connection_error(failure):
            pages.put_nowait(ListingFailure(f"作品列表请求失败：{failure}", line))

    page.on("response", on_response)  # 先开始监听，再打开主页
    page.on("requestfailed", on_request_failed)
    conclusion, profile = await _read_profile(page, progress.handle, line)
    if conclusion:
        return {"conclusion": conclusion, "profile": profile}

    listing = tiktok.Listing(datetime.fromisoformat(body["cutoff"]), progress.max_works)
    try:
        works = await _read_listing(page, pages, listing, line, progress)
    except ListingFailure as failure:
        if await _needs_login(page):
            return {"conclusion": "login_required", "profile": profile}
        failure.profile = profile
        raise
    return {"conclusion": "ok", "profile": profile, "works": works}


async def _needs_login(page: Page) -> bool:
    """读不到作品列表时看一眼页面：达人开了受众控制的，访客怎么重试都看不到。"""
    try:
        return tiktok.needs_login(await page.evaluate("document.body.innerText"))
    except BrowserError:
        return False


async def _read_profile(page: Page, handle: str, line: str) -> tuple[str | None, dict | None]:
    response = await page.goto(f"https://www.tiktok.com/@{handle}", wait_until="domcontentloaded")
    if response is not None and (wait := _http_wait(response, "达人主页")):
        raise wait
    data = await page_data(page)
    if data is None:
        raise Retry("主页没有页面数据，可能停在了挑战页", line)
    try:
        return tiktok.profile_of(data, handle)
    except ValueError as error:
        raise Retry(str(error), line) from error


async def _read_listing(
    page: Page,
    pages: asyncio.Queue[ListingPage],
    listing: tiktok.Listing,
    line: str,
    progress: CrawlProgress,
) -> list[dict]:
    progress.stage = "listing"
    empty_seen = idle = False
    read = 0
    try:
        async with asyncio.timeout(LIST_SECONDS):
            while not listing.settled:
                try:
                    reply = await asyncio.wait_for(pages.get(), WAIT_PAGE)
                except TimeoutError:
                    if idle:
                        raise ListingFailure("作品列表等待响应超时", line) from None
                    idle = True
                    await page.mouse.wheel(0, SCROLL)
                    continue
                idle = False
                if isinstance(reply, (ListingFailure, _CrawlWait)):
                    raise reply
                cursor, content = reply
                if not content:
                    if empty_seen:
                        raise ListingFailure(
                            "作品列表两次返回空正文（HTTP 200）", line, "empty_response"
                        )
                    empty_seen = True
                    if listing.started:
                        await page.mouse.wheel(0, SCROLL)
                    else:
                        response = await page.reload(wait_until="domcontentloaded")
                        if response is not None and (wait := _http_wait(response, "达人主页")):
                            raise wait
                    continue
                if listing.add(cursor, json.loads(content)):
                    progress.pages_read += 1
                    progress.works_found = listing.candidate_count
                read += 1
                if not listing.settled:
                    if read >= MAX_PAGES:
                        raise ListingFailure(
                            f"读了 {MAX_PAGES} 页还不能确定前 {listing.limit} 条", line
                        )
                    await page.mouse.wheel(0, SCROLL)
        return listing.works()
    except TimeoutError:
        raise ListingFailure(f"{LIST_SECONDS} 秒内没能确定前 {listing.limit} 条", line) from None
    except (ValueError, KeyError, TypeError, OverflowError, OSError) as error:
        raise ListingFailure(f"作品列表格式不对：{error!r}", line) from error


def _http_wait(response: Response, step: str) -> _CrawlWait | None:
    if response.status == 429:
        return _CrawlWait(
            _retry_after(response.headers.get("retry-after")), f"{step}限流（429）", "throttled"
        )
    if response.status == 407:
        return _CrawlWait(60, f"{step}代理认证失败（407）", "auth_failed")
    return None


def _retry_after(value: str | None) -> int:
    if value is None:
        return 60
    try:
        seconds = int(value)
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
        except (ValueError, TypeError, OverflowError):
            return 60
        seconds = ceil(
            (
                (date if date.tzinfo else date.replace(tzinfo=UTC)) - datetime.now(UTC)
            ).total_seconds()
        )
    return min(max(seconds, 1), MAX_RETRY_AFTER)


def _proxy_connection_error(message: str) -> bool:
    return any(
        code in message
        for code in (
            "net::ERR_PROXY_CONNECTION_FAILED",
            "net::ERR_TUNNEL_CONNECTION_FAILED",
            "net::ERR_SOCKS_CONNECTION_FAILED",
        )
    )
