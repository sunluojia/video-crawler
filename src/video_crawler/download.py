"""下载一条视频（download_video），见单视频下载方案第 2、3 节和爬取方案第 5 节。

优先复用同身份从列表拿到的媒体地址。需要读作品页时借采集线路，归还后再借下载线路拉取媒体。
下载使用同一身份的 token，传输完成后校验、落盘并上传。
"""

import errno
import hashlib
import logging
import os
import re
import shutil
import time
from collections import deque
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import httpx
from curl_cffi.requests import AsyncSession
from patchright.async_api import Error as BrowserError

from video_crawler import tiktok
from video_crawler.browser import Browsers, page_data, session_of
from video_crawler.crawl import retry_after_seconds
from video_crawler.errors import Gone, Retry, Wait
from video_crawler.identities import Identity, IdentityPool
from video_crawler.routes import proxy_reachable, proxy_url
from video_crawler.scheduler import CRAWL, DOWNLOAD, ClaimedTask, Lease, Scheduler
from video_crawler.settings import SLOW_BYTES, SLOW_WINDOW

log = logging.getLogger(__name__)

MAX_BYTES = 512 * 1024 * 1024
MIN_FREE = 10 * 1024**3
"""磁盘至少留这么多空闲，不够就等。"""
RATE_WINDOW_SECONDS = 5
MAX_TRANSFERS = 3
MEDIA_HOSTS = (
    "tiktok.com",
    "tiktokcdn.com",
    "tiktokcdn-us.com",
    "tiktokcdn-eu.com",
    "tiktokv.com",
    "tiktokv.us",
    "byteoversea.com",
    "ibytedtos.com",
    "muscdn.com",
)
WAITING = {"proxy_down", "auth_failed", "throttled"}
"""这几种线路表现属于环境问题：等环境恢复，不算尝试次数。"""


class Stop(Exception):
    """线路阶段失败。blocked 表示 token 被拒，stale 表示缓存地址应重新获取。"""

    def __init__(
        self,
        reason: str,
        outcome: str = "suspect",
        retry_after: int | None = None,
        *,
        blocked: bool = False,
        stale: bool = False,
    ) -> None:
        super().__init__(reason)
        self.reason, self.outcome, self.retry_after = reason, outcome, retry_after
        self.blocked, self.stale = blocked, stale


class _Blocked(Retry):
    """用当前身份重新取链后，媒体服务器仍然拒绝 token。"""


class Skip(Exception):
    """这个地址不行（404、5xx、不是视频），换下一个候选，不怪线路。"""


class Broken(Exception):
    """传输中途出问题（断开、太慢、大小对不上），同一地址再连一次，怪线路。"""


@dataclass
class DownloadProgress:
    task_id: str
    work_id: str
    stage: Literal["preparing", "downloading", "uploading"] = "preparing"
    received_bytes: int = 0
    total_bytes: int | None = None


class DownloadRate:
    """本机收到的视频字节，按秒汇总，保留最近一段时间的数据。"""

    def __init__(self) -> None:
        self._samples: deque[tuple[int, int]] = deque(maxlen=RATE_WINDOW_SECONDS + 1)

    def record(self, size: int, now: float) -> None:
        second = int(now)
        if self._samples and self._samples[-1][0] == second:
            self._samples[-1] = second, self._samples[-1][1] + size
        else:
            self._samples.append((second, size))

    def bytes_per_second(self, now: float) -> int:
        """取最近五个完整的秒；没有收到视频数据的秒按零计算。"""
        end = int(now)
        size = sum(
            size for second, size in self._samples if end - RATE_WINDOW_SECONDS <= second < end
        )
        return size // RATE_WINDOW_SECONDS


class Downloader:
    def __init__(
        self,
        pool: IdentityPool,
        scheduler: Scheduler,
        browsers: Browsers,
        folder: Path,
        *,
        media_cache: tiktok.MediaCache | None = None,
        slow_bytes: int = SLOW_BYTES,
        slow_window: float = SLOW_WINDOW,
    ) -> None:
        self._pool, self._scheduler, self._browsers = pool, scheduler, browsers
        self._folder = folder
        self._media = media_cache or tiktok.MediaCache()
        self._slow_bytes, self._slow_window = slow_bytes, slow_window
        self._active: dict[str, DownloadProgress] = {}
        self._rate = DownloadRate()
        folder.mkdir(parents=True, exist_ok=True)

    def progress(self) -> list[DownloadProgress]:
        return list(self._active.values())

    def download_speed(self) -> int:
        return self._rate.bytes_per_second(time.monotonic())

    async def __call__(self, task: ClaimedTask) -> None:
        """一次尝试；成功时视频已经上传，不另发事件。"""
        if shutil.disk_usage(self._folder).free < MIN_FREE:
            raise Wait(60, "磁盘空间不足")
        path = self._folder / f"{task.task_id}-{task.run}-{task.attempt_id}.mp4"
        progress = DownloadProgress(task.task_id, task.body["work"]["work_id"])
        self._active[task.attempt_id] = progress
        try:
            await self._download(task, progress, path)
        except OSError as error:
            if error.errno != errno.ENOSPC:
                raise
            raise Wait(60, "磁盘空间不足") from error
        finally:
            self._active.pop(task.attempt_id, None)
            _remove_file(path)

    async def _download(self, task: ClaimedTask, progress: DownloadProgress, path: Path) -> None:
        cached = self._media.get(progress.work_id)
        identity = self._pool.take(preferred_id=cached.identity_id if cached else None)
        result = "neutral"
        try:
            sha256 = None
            if cached and identity.id == cached.identity_id:
                sha256 = await self._fetch_media(
                    task, identity, cached.urls, path, progress, cached=cached
                )
            if sha256 is None:
                progress.stage, progress.received_bytes, progress.total_bytes = "preparing", 0, None
                async with self._route(CRAWL, task) as lease:
                    candidates = await self._address(lease, task, identity)
                sha256 = await self._fetch_media(task, identity, candidates, path, progress)
            assert sha256 is not None
            result = "ok"
        except _Blocked:
            result = "blocked"
            raise
        finally:
            self._pool.give_back(identity, result)
        progress.stage, progress.total_bytes = "uploading", progress.received_bytes
        try:
            status = await self._scheduler.upload(task, path, sha256)
        except httpx.HTTPError as error:
            raise Wait(30, f"上传时调度中心连不上：{error}") from error
        if status == 422:
            raise Retry("上传后调度中心核对不上大小或 SHA-256")
        if status not in (204, 404, 409):  # 404、409：任务取消或已有新轮次，文件不要了
            raise Wait(30, f"上传返回 {status}")

    @asynccontextmanager
    async def _route(self, use: str, task: ClaimedTask) -> AsyncIterator[Lease]:
        lease = await self._scheduler.lease(
            use, task.task_id, task.avoid, attempt_id=task.attempt_id
        )
        outcome, detail, retry_after = "normal", None, None
        try:
            yield lease
        except Stop as stop:
            outcome, detail, retry_after = stop.outcome, stop.reason, stop.retry_after
            if outcome in WAITING:
                raise Wait(retry_after or 60, detail, use) from None
            error = _Blocked if stop.blocked else Retry
            raise error(detail, lease.line) from None
        finally:
            await self._scheduler.release(lease, outcome, detail, retry_after)

    async def _fetch_media(
        self,
        task: ClaimedTask,
        identity: Identity,
        candidates: Sequence[str],
        path: Path,
        progress: DownloadProgress,
        *,
        cached: tiktok.MediaLinks | None = None,
    ) -> str | None:
        async with self._route(DOWNLOAD, task) as lease:
            blocked = False
            try:
                return await self._fetch(
                    lease, identity, task.body["work"]["url"], candidates, path, progress
                )
            except Stop as stop:
                if cached and stop.stale:
                    self._media.discard(progress.work_id, cached)
                    return None
                blocked = stop.blocked
                raise
            finally:
                self._pool.note("download", not blocked)

    async def _address(self, lease: Lease, task: ClaimedTask, identity: Identity) -> list[str]:
        """带身份 Cookie 走采集线路取地址，页面资料不足时用同线路打开 Chrome。"""
        work = task.body["work"]
        try:
            proxy = await proxy_url(lease.route)
            async with AsyncSession(
                impersonate="chrome", proxy=proxy, trust_env=False, timeout=30
            ) as http:
                response = await http.get(
                    work["url"],
                    headers={"User-Agent": identity.session.user_agent},
                    cookies=identity.session.cookies,
                )
            _check_address_status(response.status_code, response.headers.get("Retry-After"))
            if response.status_code == 200:
                with suppress(ValueError):
                    return _page_media(tiktok.scope_in_html(response.text), work["work_id"])
            async with self._browsers.open(self._pool.profile(identity), lease.route) as page:
                navigation = await page.goto(work["url"], wait_until="domcontentloaded")
                if navigation is not None:
                    _check_address_status(navigation.status, navigation.headers.get("retry-after"))
                candidates = _page_media(await page_data(page), work["work_id"])
                self._pool.refreshed(identity, await session_of(page))
                return candidates
        except LookupError as error:
            raise Gone(str(error)) from error
        except ValueError as error:
            raise Stop(str(error)) from error
        except (OSError, BrowserError) as error:  # curl_cffi 的网络错误也是 OSError
            if _proxy_auth_failed(error):
                raise Stop("读作品页时代理认证失败（407）", "auth_failed") from error
            outcome = "suspect" if await proxy_reachable(lease.route) else "proxy_down"
            raise Stop(f"读作品页失败：{type(error).__name__}", outcome) from error

    async def _fetch(
        self,
        lease: Lease,
        identity: Identity,
        referer: str,
        candidates: Sequence[str],
        path: Path,
        progress: DownloadProgress,
    ) -> str:
        """依次试候选地址，最多传输 3 次；中途断开的同一地址再连一次。返回 SHA-256。"""
        headers = {
            "User-Agent": identity.session.user_agent,
            "Referer": referer,
            "Cookie": f"tt_chain_token={identity.session.token}",
            "Accept-Encoding": "identity",
        }
        proxy = await proxy_url(lease.route)
        reason, outcome, transfers, skipped = "", "normal", 0, 0
        async with httpx.AsyncClient(
            proxy=proxy,
            trust_env=False,
            headers=headers,
            timeout=httpx.Timeout(self._slow_window, connect=10),
            follow_redirects=True,
            max_redirects=3,
            event_hooks={"request": [trusted]},
        ) as client:
            for url in candidates:
                for _ in range(2):
                    if transfers == MAX_TRANSFERS:
                        break
                    transfers += 1
                    try:
                        return await transfer(
                            client,
                            url,
                            path,
                            progress,
                            rate=self._rate,
                            slow_bytes=self._slow_bytes,
                            slow_window=self._slow_window,
                        )
                    except Skip as error:
                        reason = str(error)
                        skipped += 1
                        break
                    except Broken as error:
                        reason, outcome = str(error), "suspect"
                    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ProxyError) as error:
                        if _proxy_auth_failed(error):
                            raise Stop("代理认证失败（407）", "auth_failed") from error
                        if not await proxy_reachable(lease.route):
                            raise Stop("代理连不上", "proxy_down") from error
                        reason, outcome = f"连不上视频服务器：{type(error).__name__}", "suspect"
                    except httpx.HTTPError as error:
                        reason, outcome = f"传输出错：{type(error).__name__}", "suspect"
        raise Stop(reason or "没有可用的媒体地址", outcome, stale=skipped == len(candidates))


def _page_media(data: dict | None, work_id: str) -> list[str]:
    if data is None:
        raise ValueError("作品页没有页面数据，可能停在了挑战页")
    candidates = tiktok.media_of(data, work_id)
    if not candidates:
        raise ValueError("作品页里没有媒体地址")
    return candidates


def _check_address_status(status: int, retry_after: str | None) -> None:
    if status == 407:
        raise Stop("读作品页时代理认证失败（407）", "auth_failed")
    if status == 429:
        raise Stop("读作品页被限流（429）", "throttled", retry_after_seconds(retry_after))


def _proxy_auth_failed(error: Exception) -> bool:
    return re.search(r"\b407\b", str(error)) is not None


def _remove_file(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as error:
        log.warning("无法清理下载文件 %s：%s", path, error)


async def transfer(
    client: httpx.AsyncClient,
    url: str,
    path: Path,
    progress: DownloadProgress | None = None,
    *,
    rate: DownloadRate | None = None,
    slow_bytes: int = SLOW_BYTES,
    slow_window: float = SLOW_WINDOW,
) -> str:
    """完整下载一次：只认 200 的视频响应，边写 .part 边算 SHA-256，核对长度后改名。"""
    part = path.with_suffix(".part")
    digest, size = hashlib.sha256(), 0
    if progress is not None:
        progress.stage, progress.received_bytes, progress.total_bytes = "downloading", 0, None
    try:
        async with client.stream("GET", url) as response:
            status = response.status_code
            if status in (401, 403):
                raise Stop(f"视频服务器拒绝（{status}）", blocked=True, stale=status == 403)
            if status == 407:
                raise Stop("代理认证失败（407）", "auth_failed")
            if status == 429:
                retry_after = retry_after_seconds(response.headers.get("Retry-After"))
                raise Stop("视频服务器限流（429）", "throttled", retry_after)
            if status == 206:
                raise Broken("收到部分内容（206）")
            if status != 200:
                raise Skip(f"视频服务器返回 {status}")
            length = response.headers.get("Content-Length")
            total = int(length) if length else None
            if total is not None and total > MAX_BYTES:
                raise Gone("文件超过 512 MiB 上限")
            if total is None and "chunked" not in response.headers.get("Transfer-Encoding", ""):
                raise Broken("响应没有可核对的长度")
            if progress is not None and total is not None and total > 0:
                progress.total_bytes = total
            kind = response.headers.get("Content-Type", "").split(";")[0].strip()
            window, window_bytes = time.monotonic(), 0
            head = bytearray()
            video = _video(kind, head)
            with part.open("wb") as file:
                async for chunk in response.aiter_raw():
                    if not video:
                        head.extend(chunk[: 8 - len(head)])
                        if len(head) == 8:
                            video = _video(kind, head)
                            if not video:
                                raise Skip(f"返回的不是视频（{kind or '没有类型'}）")
                    chunk_size = len(chunk)
                    size += chunk_size
                    if size > MAX_BYTES:
                        raise Gone("文件超过 512 MiB 上限")
                    file.write(chunk)
                    digest.update(chunk)
                    if progress is not None:
                        progress.received_bytes = size
                    received_at = time.monotonic()
                    if rate is not None:
                        rate.record(chunk_size, received_at)
                    window_bytes += chunk_size
                    if (elapsed := received_at - window) >= slow_window:
                        if window_bytes / elapsed < slow_bytes:
                            raise Broken("下载太慢")
                        window, window_bytes = received_at, 0
                file.flush()
                os.fsync(file.fileno())
        if not size or (total is not None and size != total):
            raise Broken(f"收到 {size} 字节，声明 {length or '未知'}")
        if not video:
            raise Skip(f"返回的不是视频（{kind or '没有类型'}）")
        part.replace(path)
        return digest.hexdigest()
    except (httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.ReadError) as error:
        raise Broken(f"传输中途断开：{type(error).__name__}") from error
    finally:
        _remove_file(part)


def _video(kind: str, head: bytes | bytearray) -> bool:
    """视频响应；通用二进制响应看文件头是不是 MP4。"""
    if kind.startswith("video/"):
        return True
    return kind in ("", "application/octet-stream", "binary/octet-stream") and head[4:8] == b"ftyp"


async def trusted(request: httpx.Request) -> None:
    """每一跳都要是 https 和受信任的媒体域名。"""
    host = request.url.host
    if request.url.scheme != "https" or not any(
        host == name or host.endswith(f".{name}") for name in MEDIA_HOSTS
    ):
        raise Skip(f"不受信任的媒体地址：{host}")
