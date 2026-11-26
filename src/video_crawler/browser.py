"""按需启动本机 Chrome：达人采集可复用同一身份和线路的上下文。

用 patchright（去掉自动化痕迹的 Playwright），按它推荐的方式启动：本机 Chrome、有界面、
不改窗口大小。patchright 出问题时可以换回原版 Playwright，接口相同。
"""

import asyncio
import json
import logging
import re
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass
from hashlib import sha256
from operator import itemgetter
from pathlib import Path

from patchright.async_api import BrowserContext, Page, Playwright, Route, async_playwright
from patchright.async_api import Error as BrowserError

from video_crawler import tiktok
from video_crawler.routes import browser_proxy
from video_crawler.settings import (
    BLOCKED_RESOURCE_TYPES,
    BROWSER_CONTEXT_IDLE_TTL_SECONDS,
    BROWSER_CONTEXT_MAX_USES,
    BlockableResource,
)

log = logging.getLogger(__name__)
BLANK = "about:blank"


@dataclass
class Session:
    """浏览器会话里取出来的东西，只放在内存里。"""

    cookies: dict[str, str]
    user_agent: str
    browser: str
    """Chrome 主版本。token 只对签发它的浏览器有效，版本变了身份要重建。"""

    @property
    def token(self) -> str | None:
        return self.cookies.get("tt_chain_token")


@dataclass
class _HotContext:
    context: BrowserContext
    blank: Page
    resources: AsyncExitStack
    route_key: tuple[str, bytes]
    uses: int = 0
    failed: bool = False
    idle_timer: asyncio.TimerHandle | None = None
    closing: asyncio.Task[None] | None = None

    def cancel_idle_timer(self) -> None:
        if self.idle_timer is not None:
            self.idle_timer.cancel()
            self.idle_timer = None

    @property
    def healthy(self) -> bool:
        return (
            not self.failed
            and self.closing is None
            and not self.context.is_closed()
            and not self.blank.is_closed()
            and self.blank.url == BLANK
        )


class Browsers:
    def __init__(
        self,
        channel: str,
        warm_headless: bool = False,
        *,
        block_nonessential_resources: bool = True,
        blocked_resource_types: tuple[BlockableResource, ...] = BLOCKED_RESOURCE_TYPES,
        reuse_browser_context: bool = True,
        browser_context_max_uses: int = BROWSER_CONTEXT_MAX_USES,
        browser_context_idle_ttl_seconds: float = BROWSER_CONTEXT_IDLE_TTL_SECONDS,
    ) -> None:
        self._channel = channel
        self._blocked_resource_types = frozenset(
            blocked_resource_types if block_nonessential_resources else ()
        )
        self.limit = 0
        """同时开的 Chrome 上限：采集并发加预热并发，随心跳调整。"""
        self._open = 0
        self._changed = asyncio.Event()
        self._stopping = False
        self._reuse_context = reuse_browser_context
        self._max_uses = browser_context_max_uses
        self._idle_ttl = browser_context_idle_ttl_seconds
        # 资料目录对应身份。采集、预热和取链都按目录互斥，切换代理时也一样。
        self._borrowed: set[Path] = set()
        self._hot: dict[Path, _HotContext] = {}
        self._warm_headless = warm_headless
        """预热（只拿 Cookie）可以用无头：实测 5.6–7.9 秒就能拿到 tt_chain_token。
        采集作品必须保留有界面：同一个资料目录、同一条线路，无头拿作品接口返回 403，
        有界面返回 200（可能空正文）。两者不能混用。"""
        self._playwright: Playwright | None = None

    async def start(self) -> None:
        self._playwright = await async_playwright().start()
        self._stopping = False

    async def stop(self) -> None:
        self._stopping = True
        self._changed.set()
        try:
            await asyncio.gather(*(self._close_hot(key, hot) for key, hot in self._hot.items()))
        finally:
            if self._playwright is not None:
                await self._playwright.stop()
                self._playwright = None

    async def resize(self, limit: int) -> None:
        self.limit = limit
        excess = self._open - self._closing_count() - limit
        if excess > 0:
            for key, hot in self._oldest_idle()[:excess]:
                self._close_hot(key, hot)
        self._changed.set()

    async def _wait_for_change(self) -> None:
        # 池状态只在事件循环线程修改。清除通知前的状态检查与下面等待之间没有挂起点。
        if self._stopping:
            raise BrowserError("浏览器正在停止")
        self._changed.clear()
        await self._changed.wait()

    def _oldest_idle(self) -> list[tuple[Path, _HotContext]]:
        idle = [
            (timer.when(), key, hot)
            for key, hot in self._hot.items()
            if (timer := hot.idle_timer) is not None
        ]
        return [(key, hot) for _, key, hot in sorted(idle, key=itemgetter(0))]

    def _closing_count(self) -> int:
        return sum(hot.closing is not None for hot in self._hot.values())

    @asynccontextmanager
    async def _slot(self) -> AsyncIterator[None]:
        """空闲 Chrome 也占槽；有新的使用者时，先腾出最久未用的槽位。"""
        while not self._stopping and self._open >= self.limit:
            if (
                self.limit
                and self._open - self._closing_count() >= self.limit
                and (idle := self._oldest_idle())
            ):
                self._close_hot(*idle[0])
            await self._wait_for_change()
        if self._stopping:
            raise BrowserError("浏览器正在停止")
        self._open += 1
        try:
            yield
        finally:
            self._open -= 1
            self._changed.set()

    @asynccontextmanager
    async def _reserve(self, profile: Path) -> AsyncIterator[tuple[Path, _HotContext | None]]:
        key = profile.resolve()
        hot = self._hot.get(key)
        while key in self._borrowed or (hot is not None and hot.closing is not None):
            await self._wait_for_change()
            hot = self._hot.get(key)
        if self._stopping:
            raise BrowserError("浏览器正在停止")
        self._borrowed.add(key)
        if hot is not None and (timer := hot.idle_timer) is not None:
            if timer.when() <= asyncio.get_running_loop().time():
                hot.failed = True
            hot.cancel_idle_timer()
        try:
            yield key, hot
        finally:
            self._borrowed.remove(key)
            self._changed.set()

    def invalidate(self, profile: Path) -> None:
        """会话要重新预热时淘汰采集 Chrome；正在使用的等归还再关。"""
        if self._hot and (hot := self._hot.get(key := profile.resolve())) is not None:
            hot.failed = True
            if hot.idle_timer is not None:
                self._close_hot(key, hot)

    def _close_hot(self, key: Path, hot: _HotContext) -> asyncio.Task[None]:
        if hot.closing is None:
            hot.cancel_idle_timer()
            hot.closing = asyncio.create_task(self._dispose_hot(key, hot))
        return hot.closing

    async def _dispose_hot(self, key: Path, hot: _HotContext) -> None:
        try:
            await hot.resources.aclose()
        except (BrowserError, OSError) as error:
            log.warning("crawl_context_close_failed error=%s", type(error).__name__)
        finally:
            del self._hot[key]
            self._changed.set()

    async def _headless_agent(self) -> str:
        """无头时 Chrome 的 UA 带 HeadlessChrome，换成正常写法；版本和系统取本机 Chrome 的，
        这样预热和有界面采集时的 UA 一致，Chrome 升级后也跟着变。"""
        browser = await self._playwright.chromium.launch(channel=self._channel, headless=True)
        try:
            page = await browser.new_page()
            return (await page.evaluate("navigator.userAgent")).replace("HeadlessChrome", "Chrome")
        finally:
            await browser.close()

    @asynccontextmanager
    async def _persistent_context(
        self, profile: Path, route: dict, *, headless: bool = False
    ) -> AsyncIterator[BrowserContext]:
        async with self._slot(), browser_proxy(route) as proxy:
            options: dict = {}
            if proxy is None:
                options["args"] = ["--no-proxy-server"]  # 直连线路也忽略系统代理和 PAC
            if headless:
                options["user_agent"] = await self._headless_agent()
            launch = asyncio.create_task(
                self._playwright.chromium.launch_persistent_context(
                    str(profile),
                    channel=self._channel,
                    headless=headless,
                    no_viewport=True,
                    proxy=proxy,
                    **options,
                )
            )
            try:
                context = await asyncio.shield(launch)
            except asyncio.CancelledError:
                # 启动命令已经交给驱动，先收回 Chrome 才能再借同一个资料目录。
                with suppress(BrowserError, OSError):
                    await (await launch).close()
                raise
            try:
                yield context
            finally:
                close = asyncio.create_task(context.close())
                try:
                    await asyncio.shield(close)
                except asyncio.CancelledError:
                    await asyncio.gather(close, return_exceptions=True)
                    raise

    @asynccontextmanager
    async def open(
        self, profile: Path, route: dict, *, warming: bool = False
    ) -> AsyncIterator[Page]:
        """首页预热和下载回退取链使用独立的 Chrome，退出即关浏览器和转发口。"""
        async with self._reserve(profile) as (key, hot):
            if hot is not None:
                await asyncio.shield(self._close_hot(key, hot))
            async with self._persistent_context(
                profile, route, headless=self._warm_headless and warming
            ) as context:
                yield context.pages[0] if context.pages else await context.new_page()

    @asynccontextmanager
    async def open_crawl(self, profile: Path, route: dict, *, line: str) -> AsyncIterator[Page]:
        """达人采集的导航入口。预热和下载回退取链使用 open。"""
        acquire = (
            self._open_hot(profile, route, line)
            if self._reuse_context
            else self.open(profile, route)
        )
        async with acquire as page, self._crawl_resources(page):
            yield page

    async def _new_hot(
        self, key: Path, profile: Path, route: dict, route_key: tuple[str, bytes]
    ) -> _HotContext:
        async with AsyncExitStack() as resources:
            context = await resources.enter_async_context(self._persistent_context(profile, route))
            blank = next((page for page in context.pages if page.url == BLANK), None)
            if blank is None:
                blank = await context.new_page()
            await self._close_other_tabs(context, blank)
            if self._stopping:
                raise BrowserError("浏览器正在停止")
            hot = _HotContext(context, blank, resources.pop_all(), route_key)
            self._hot[key] = hot

        def disconnected(_: BrowserContext) -> None:
            self._close_hot(key, hot)

        context.on("close", disconnected)
        hot.resources.callback(context.remove_listener, "close", disconnected)
        return hot

    @asynccontextmanager
    async def _open_hot(self, profile: Path, route: dict, line: str) -> AsyncIterator[Page]:
        route_key = line, sha256(json.dumps(route, sort_keys=True).encode()).digest()
        async with self._reserve(profile) as (key, hot):
            if hot is not None and (not hot.healthy or hot.route_key != route_key):
                await asyncio.shield(self._close_hot(key, hot))
                hot = None
            if hot is None:
                hot = await self._new_hot(key, profile, route, route_key)
            hot.uses += 1

            def crashed(_: Page) -> None:
                hot.failed = True

            try:
                page = await hot.context.new_page()
                page.on("crash", crashed)
                try:
                    yield page
                finally:
                    page.remove_listener("crash", crashed)
                    if page.is_closed():
                        hot.failed = True
            except (BrowserError, asyncio.CancelledError):
                hot.failed = True
                raise
            finally:
                await self._return_hot(key, hot)

    async def _return_hot(self, key: Path, hot: _HotContext) -> None:
        keep = False
        try:
            if not self._stopping and hot.healthy and hot.uses < self._max_uses:
                await self._close_other_tabs(hot.context, hot.blank)
                keep = hot.healthy and self._open <= self.limit and hot.context.pages == [hot.blank]
        except BrowserError:
            pass  # 标签页关闭失败时直接回收整个 Chrome。
        finally:
            if keep:
                hot.idle_timer = asyncio.get_running_loop().call_later(
                    self._idle_ttl, self._close_hot, key, hot
                )
            else:
                await asyncio.shield(self._close_hot(key, hot))

    @staticmethod
    async def _close_other_tabs(context: BrowserContext, blank: Page) -> None:
        for page in list(context.pages):
            if page is not blank:
                await page.close()

    @asynccontextmanager
    async def _crawl_resources(self, page: Page) -> AsyncIterator[None]:
        enabled = bool(self._blocked_resource_types)
        blocked = 0

        async def filter_resource(route: Route) -> None:
            nonlocal blocked
            if route.request.resource_type in self._blocked_resource_types:
                blocked += 1
                await route.abort()
            else:
                await route.continue_()

        try:
            if enabled:
                await page.route("**/*", filter_resource)
            yield
        finally:
            if enabled:
                with suppress(BrowserError):
                    await page.unroute_all(behavior="ignoreErrors")
            log.info("crawl_resources enabled=%s blocked=%d", enabled, blocked)


async def session_of(page: Page) -> Session:
    cookies = await page.context.cookies(tiktok.HOME)
    agent = await page.evaluate("navigator.userAgent")
    version = re.search(r"Chrome/(\d+)", agent)
    return Session(
        {cookie["name"]: cookie["value"] for cookie in cookies},
        agent,
        (version and version[1]) or "",
    )


async def page_data(page: Page) -> dict | None:
    return tiktok.scope(await page.evaluate(tiktok.DATA_SCRIPT))
