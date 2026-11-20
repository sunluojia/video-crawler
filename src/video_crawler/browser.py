"""按需启动本机 Chrome：一个身份一个资料目录，用完就关，目录留在磁盘上。

用 patchright（去掉自动化痕迹的 Playwright），按它推荐的方式启动：本机 Chrome、有界面、
不改窗口大小。patchright 出问题时可以换回原版 Playwright，接口相同。
"""

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from patchright.async_api import Page, Playwright, async_playwright

from video_crawler import tiktok
from video_crawler.routes import browser_proxy


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


class Browsers:
    def __init__(self, channel: str, warm_headless: bool = False) -> None:
        self._channel = channel
        self.limit = 0
        """同时开的 Chrome 上限：采集并发加预热并发，随心跳调整。"""
        self._open = 0
        self._changed = asyncio.Condition()
        self._warm_headless = warm_headless
        """预热（只拿 Cookie）可以用无头：实测 5.6–7.9 秒就能拿到 tt_chain_token。
        采集作品必须保留有界面：同一个资料目录、同一条线路，无头拿作品接口返回 403，
        有界面返回 200（可能空正文）。两者不能混用。"""
        self._playwright: Playwright | None = None

    async def start(self) -> None:
        self._playwright = await async_playwright().start()

    async def stop(self) -> None:
        await self._playwright.stop()

    async def resize(self, limit: int) -> None:
        async with self._changed:
            self.limit = limit
            self._changed.notify_all()

    @asynccontextmanager
    async def _slot(self) -> AsyncIterator[None]:
        """等开着的 Chrome 少于上限再占一个；上限调小时，开着的用完为止。"""
        async with self._changed:
            await self._changed.wait_for(lambda: self._open < self.limit)
            self._open += 1
        try:
            yield
        finally:
            async with self._changed:
                self._open -= 1
                self._changed.notify_all()

    @asynccontextmanager
    async def open(
        self, profile: Path, route: dict, *, warming: bool = False
    ) -> AsyncIterator[Page]:
        """用这个资料目录启动 Chrome，经线路访问；退出时关掉浏览器和转发口。

        warming=True 表示这一步只是预热拿 Cookie，可以按 warm_headless 决定要不要界面。
        """
        headless = self._warm_headless and warming
        async with self._slot(), browser_proxy(route) as proxy:
            options: dict = {}
            if proxy is None:
                options["args"] = ["--no-proxy-server"]  # 直连线路也忽略系统代理和 PAC
            if headless:
                # 无头时 Chrome 的 UA 会带 HeadlessChrome，换成正常 UA。
                options["user_agent"] = (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36"
                )
            context = await self._playwright.chromium.launch_persistent_context(
                str(profile),
                channel=self._channel,
                headless=headless,
                no_viewport=True,
                proxy=proxy,
                **options,
            )
            try:
                yield context.pages[0] if context.pages else await context.new_page()
            finally:
                await context.close()


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
