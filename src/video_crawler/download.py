"""下载一条视频（download_video），见单视频下载方案第 2、3 节和爬取方案第 5 节。

优先复用同身份从列表拿到的媒体地址。需要读作品页时借采集线路，归还后再借下载线路拉取媒体。
拉流只使用同一身份的 UA 和 token 快照，传输完成后校验、落盘并上传。
"""

import asyncio
import errno
import hashlib
import logging
import os
import re
import shutil
import time
from collections import deque
from collections.abc import AsyncIterator, Sequence
from contextlib import ExitStack, asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import httpx
from curl_cffi.requests import AsyncSession
from patchright.async_api import Error as BrowserError

from video_crawler import tiktok
from video_crawler.browser import Browsers, Session, page_data, session_of
from video_crawler.crawl import retry_after_seconds
from video_crawler.errors import Gone, Retry, Wait
from video_crawler.identities import DownloadCredentials, Identity, IdentityPool
from video_crawler.preextract import Bundle, Preextractor
from video_crawler.routes import proxy_reachable, proxy_url
from video_crawler.scheduler import CRAWL, DOWNLOAD, ClaimedTask, Lease, Scheduler
from video_crawler.settings import (
    MAX_VIDEO_HEIGHT,
    RANGE_MIN_BYTES,
    RANGE_PARTS,
    SLOW_BYTES,
    SLOW_WINDOW,
)

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
GENERIC_MEDIA = {"", "application/octet-stream", "binary/octet-stream"}


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


class _Received:
    """整条视频共享进度和低速窗口，分片的字节也计入同一个窗口。"""

    def __init__(
        self,
        progress: DownloadProgress | None,
        rate: DownloadRate | None,
        slow_bytes: int,
        slow_window: float,
    ) -> None:
        self.progress, self.rate = progress, rate
        self.slow_bytes, self.slow_window = slow_bytes, slow_window
        self.reset()

    def reset(self, total: int | None = None) -> None:
        self.size, self.window_bytes, self.window = 0, 0, time.monotonic()
        if self.progress is not None:
            self.progress.stage = "downloading"
            self.progress.received_bytes, self.progress.total_bytes = 0, total or None

    def record(self, size: int) -> None:
        self.size += size
        if self.progress is not None:
            self.progress.received_bytes = self.size
        now = time.monotonic()
        if self.rate is not None:
            self.rate.record(size, now)
        self.window_bytes += size
        if (elapsed := now - self.window) >= self.slow_window:
            if self.window_bytes / elapsed < self.slow_bytes:
                raise Broken("下载太慢")
            self.window, self.window_bytes = now, 0


class Downloader:
    def __init__(
        self,
        pool: IdentityPool,
        scheduler: Scheduler,
        browsers: Browsers,
        folder: Path,
        *,
        media_cache: tiktok.MediaCache | None = None,
        max_video_height: int = MAX_VIDEO_HEIGHT,
        slow_bytes: int = SLOW_BYTES,
        slow_window: float = SLOW_WINDOW,
        range_parts: int = RANGE_PARTS,
        range_min_bytes: int = RANGE_MIN_BYTES,
        preextractor: Preextractor | None = None,
    ) -> None:
        self._pool, self._scheduler, self._browsers = pool, scheduler, browsers
        self._folder, self._preextractor = folder, preextractor
        self._media = media_cache or tiktok.MediaCache()
        self._max_video_height = max_video_height
        self._slow_bytes, self._slow_window = slow_bytes, slow_window
        self._range_parts, self._range_min_bytes = range_parts, range_min_bytes
        self._active: dict[str, DownloadProgress] = {}
        self._rate = DownloadRate()
        folder.mkdir(parents=True, exist_ok=True)

    def progress(self) -> list[DownloadProgress]:
        return list(self._active.values())

    def download_speed(self) -> int:
        return self._rate.bytes_per_second(time.monotonic())

    def has_cached_media(self, work_id: str | None = None) -> bool:
        if work_id is None:
            return self._media.has_usable(
                lambda key: self._pool.download_credentials(key) is not None
            )
        cached = self._media.get(work_id)
        return bool(cached and self._pool.download_credentials(cached.identity_id))

    async def __call__(self, task: ClaimedTask) -> None:
        """一次尝试；成功时视频已经上传，不另发事件。"""
        if shutil.disk_usage(self._folder).free < MIN_FREE:
            raise Wait(60, "磁盘空间不足", DOWNLOAD)
        path = self._folder / f"{task.task_id}-{task.run}-{task.attempt_id}.mp4"
        progress = DownloadProgress(task.task_id, task.body["work"]["work_id"])
        self._active[task.attempt_id] = progress
        try:
            await self._download(task, progress, path)
        except OSError as error:
            if error.errno != errno.ENOSPC:
                raise
            raise Wait(60, "磁盘空间不足", DOWNLOAD) from error
        finally:
            self._active.pop(task.attempt_id, None)
            _remove_file(path)

    async def _download(self, task: ClaimedTask, progress: DownloadProgress, path: Path) -> None:
        cached = self._media.get(progress.work_id)
        credentials = (
            self._pool.download_credentials(cached.identity_id)
            if cached and cached.identity_id
            else None
        )
        if credentials is None and hasattr(self._media, "get_shared"):
            shared = await asyncio.to_thread(self._media.get_shared, progress.work_id)
            if shared and shared.credentials:
                cached, credentials = shared, shared.credentials
        sha256 = None
        if cached and credentials:
            sha256 = await self._fetch_media(
                task, credentials, cached.identity_id, cached.urls, path, progress, cached=cached
            )
        if sha256 is None:
            progress.stage, progress.received_bytes, progress.total_bytes = "preparing", 0, None
            candidates, credentials, identity_id = await self._prepare(task)
            sha256 = await self._fetch_media(
                task, credentials, identity_id, candidates, path, progress
            )
        assert sha256 is not None
        if self._preextractor is None:
            await self._upload(task, progress, path, sha256)
            return

        bundle_path = path.with_suffix(".tar")
        try:
            original_size = path.stat().st_size
            started = time.monotonic()
            bundle = await self._preextractor.create(path, bundle_path, sha256)
            path.unlink()
            prepare_seconds = time.monotonic() - started
            await self._upload(
                task, progress, bundle_path, bundle.sha256,
                metrics=(original_size, bundle, prepare_seconds),
            )  # fmt: skip
        finally:
            _remove_file(bundle_path)

    async def _upload(
        self,
        task: ClaimedTask,
        progress: DownloadProgress,
        path: Path,
        sha256: str,
        *,
        metrics: tuple[int, Bundle, float] | None = None,
    ) -> None:
        progress.stage, progress.total_bytes = "uploading", progress.received_bytes
        started = time.monotonic()
        status = None
        try:
            status = await self._scheduler.upload(task, path, sha256)
        except httpx.HTTPError as error:
            raise Wait(30, f"上传时调度中心连不上：{error}") from error
        finally:
            if metrics is not None:
                original_size, bundle, prepare_seconds = metrics
                log.info(
                    "预抽取 %s mp4_kib=%.1f tar_kib=%.1f ratio=%.2f "
                    "extract_s=%.3f prepare_s=%.3f upload_s=%.3f upload_status=%s",
                    task.task_id, original_size / 1024, bundle.size / 1024,
                    original_size / bundle.size, bundle.extract_seconds, prepare_seconds,
                    time.monotonic() - started,
                    status if status is not None else "interrupted",
                )  # fmt: skip
        if status == 422:
            raise Retry("上传后调度中心核对不上大小或 SHA-256")
        if status not in (204, 404, 409):  # 404、409：任务取消或已有新轮次，文件不要了
            raise Wait(30, f"上传返回 {status}", DOWNLOAD)

    async def _prepare(self, task: ClaimedTask) -> tuple[list[str], DownloadCredentials, str]:
        identity = await self._pool.take_for_download()
        with ExitStack() as borrow:
            borrow.callback(self._pool.give_back, identity, "neutral")
            session = identity.session
            assert session is not None
            async with self._route(CRAWL, task) as lease:
                try:
                    candidates = await self._address(lease, task, identity, session)
                    credentials = self._pool.download_credentials(identity.id)
                    if credentials is None:
                        raise Wait(10, "取链时身份会话失效")
                    return candidates, credentials, identity.id
                finally:
                    # 浏览器已关闭，可以先释放身份，再等待采集租约归还。
                    borrow.close()

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
            raise Retry(detail, lease.line) from None
        finally:
            await self._scheduler.release(lease, outcome, detail, retry_after)

    async def _fetch_media(
        self,
        task: ClaimedTask,
        credentials: DownloadCredentials,
        identity_id: str,
        candidates: Sequence[str],
        path: Path,
        progress: DownloadProgress,
        *,
        cached: tiktok.MediaLinks | None = None,
    ) -> str | None:
        async with self._route(DOWNLOAD, task) as lease:
            blocked = succeeded = stale_cached = False
            try:
                sha256 = await self._fetch(
                    lease, credentials, task.body["work"]["url"], candidates, path, progress
                )
                succeeded = True
                return sha256
            except Stop as stop:
                blocked = stop.blocked
                if cached and (stop.stale or (not identity_id and stop.blocked)):
                    stale_cached = True
                    await asyncio.to_thread(self._media.discard, progress.work_id, cached)
                    return None
                raise
            finally:
                if stale_cached or not identity_id:
                    # 过期或跨机共享直链只计入下载统计，不扣本机身份失败次数。
                    self._pool.note("download", not blocked)
                else:
                    self._pool.record_download(
                        identity_id, blocked=blocked, succeeded=succeeded, token=credentials[1]
                    )

    async def _address(
        self, lease: Lease, task: ClaimedTask, identity: Identity, session: Session
    ) -> list[str]:
        """带身份 Cookie 走采集线路取地址，页面资料不足时用同线路打开 Chrome。"""
        work = task.body["work"]
        try:
            proxy = await proxy_url(lease.route)
            async with AsyncSession(
                impersonate="chrome", proxy=proxy, trust_env=False, timeout=30
            ) as http:
                response = await http.get(
                    work["url"],
                    headers={"User-Agent": session.user_agent},
                    cookies=session.cookies,
                )
            _check_address_status(response.status_code, response.headers.get("Retry-After"))
            if response.status_code == 200:
                with suppress(ValueError):
                    return _page_media(
                        tiktok.scope_in_html(response.text), work["work_id"], self._max_video_height
                    )
            async with self._browsers.open(self._pool.profile(identity), lease.route) as page:
                navigation = await page.goto(work["url"], wait_until="domcontentloaded")
                if navigation is not None:
                    status = navigation.status
                    _check_address_status(status, navigation.headers.get("retry-after"))
                    if status == 403 or status >= 500:
                        raise Stop(f"读作品页返回 HTTP {status}")
                data = await page_data(page)
                try:
                    candidates = _page_media(data, work["work_id"], self._max_video_height)
                except (ValueError, tiktok.NoPlaybackAddress) as error:
                    if isinstance(error, tiktok.BlockedVideoPage):
                        raise
                    notice = ""
                    with suppress(BrowserError):
                        notice = await page.evaluate("document.body?.innerText ?? ''")
                    if reason := tiktok.video_access_notice(notice):
                        raise Gone(reason) from None
                    raise
                self._pool.refreshed(identity, await session_of(page))
                return candidates
        except tiktok.VideoUnavailable as error:
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
        credentials: DownloadCredentials,
        referer: str,
        candidates: Sequence[str],
        path: Path,
        progress: DownloadProgress,
    ) -> str:
        """依次试候选地址，最多传输 3 次；中途断开的同一地址再连一次。返回 SHA-256。"""
        user_agent, token = credentials
        headers = {
            "User-Agent": user_agent,
            "Referer": referer,
            "Cookie": f"tt_chain_token={token}",
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
                            range_parts=self._range_parts,
                            range_min_bytes=self._range_min_bytes,
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


def _page_media(data: dict | None, work_id: str, max_video_height: int) -> list[str]:
    if data is None:
        raise ValueError("作品页没有页面数据，可能停在了挑战页")
    candidates = tiktok.media_of(data, work_id, max_video_height)
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
    range_parts: int = RANGE_PARTS,
    range_min_bytes: int = RANGE_MIN_BYTES,
) -> str:
    """完整下载一次，核对大小及 SHA-256 后再把临时文件改名。"""
    part = path.with_suffix(".part")
    received = _Received(progress, rate, slow_bytes, slow_window)
    digest = None
    try:
        async with client.stream("GET", url) as response:
            total, kind = _file_info(response)
            if (
                range_parts > 1
                and total is not None
                and total >= max(range_min_bytes, range_parts)
                and any(
                    unit.lower() == "bytes"
                    for unit in response.headers.get_list("Accept-Ranges", split_commas=True)
                )
                and _identity_encoding(response)
                and (kind.startswith("video/") or kind in GENERIC_MEDIA)
            ):
                range_url = response.url
                etag = response.headers.get("ETag", "")
            else:
                digest = await _single(response, part, received, total, kind)
        if digest is None:
            assert total is not None
            try:
                digest = await _ranges(
                    client, range_url, part, received, total, kind, range_parts, etag
                )
            except (Broken, Skip, httpx.HTTPError) as error:
                log.debug("视频分片未完成，回退到单流：%s", type(error).__name__)
                async with client.stream("GET", url) as response:
                    digest = await _single(response, part, received, *_file_info(response))
        part.replace(path)
        return digest
    except (httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.ReadError) as error:
        raise Broken(f"传输中途断开：{type(error).__name__}") from error
    finally:
        _remove_file(part)


def _file_info(response: httpx.Response) -> tuple[int | None, str]:
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
    try:
        total = int(length) if length is not None else None
    except ValueError as error:
        raise Broken("响应的文件长度无效") from error
    if total is None and "chunked" not in response.headers.get("Transfer-Encoding", ""):
        raise Broken("响应没有可核对的长度")
    if total is not None and total < 0:
        raise Broken("响应的文件长度无效")
    if total is not None and total > MAX_BYTES:
        raise Gone("文件超过 512 MiB 上限")
    return total, response.headers.get("Content-Type", "").split(";")[0].strip().lower()


async def _single(
    response: httpx.Response, part: Path, received: _Received, total: int | None, kind: str
) -> str:
    received.reset(total)
    digest = hashlib.sha256()
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
            if received.size + len(chunk) > MAX_BYTES:
                raise Gone("文件超过 512 MiB 上限")
            file.write(chunk)
            digest.update(chunk)
            received.record(len(chunk))
        file.flush()
        os.fsync(file.fileno())
    if not received.size or (total is not None and received.size != total):
        declared = total if total is not None else "未知"
        raise Broken(f"收到 {received.size} 字节，声明 {declared}")
    if not video:
        raise Skip(f"返回的不是视频（{kind or '没有类型'}）")
    return digest.hexdigest()


def _identity_encoding(response: httpx.Response) -> bool:
    return response.headers.get("Content-Encoding", "identity").lower() == "identity"


async def _ranges(
    client: httpx.AsyncClient,
    url: httpx.URL,
    part: Path,
    received: _Received,
    total: int,
    kind: str,
    count: int,
    etag: str,
) -> str:
    received.reset(total)
    with part.open("wb") as file:
        file.truncate(total)
    validator = {"If-Range": etag} if etag.startswith('"') else {}

    async def fetch(start: int, end: int) -> None:
        headers = validator | {"Range": f"bytes={start}-{end}"}
        expected = end - start + 1
        async with client.stream("GET", url, headers=headers) as response:
            if response.status_code != 206:
                raise Broken(f"分片返回 {response.status_code}")
            if (
                response.headers.get("Content-Range", "").lower() != f"bytes {start}-{end}/{total}"
                or response.headers.get("Content-Length", str(expected)) != str(expected)
                or (validator and response.headers.get("ETag", etag) != etag)
                or not _identity_encoding(response)
            ):
                raise Broken("分片响应与原文件不符")
            with part.open("r+b") as file:
                file.seek(start)
                size = 0
                async for chunk in response.aiter_raw():
                    size += len(chunk)
                    if size > expected:
                        raise Broken("分片超过声明的范围")
                    file.write(chunk)
                    received.record(len(chunk))
            if size != expected:
                raise Broken("分片未收完整")

    jobs = [
        asyncio.create_task(fetch(index * total // count, (index + 1) * total // count - 1))
        for index in range(count)
    ]
    try:
        await asyncio.gather(*jobs)
    finally:
        for job in jobs:
            if not job.done():
                job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)

    digest = hashlib.sha256()
    with part.open("r+b") as file:
        if not _video(kind, file.read(8)):
            raise Broken("分片拼接后没有有效的视频文件头")
        file.seek(0)
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
            await asyncio.sleep(0)
        os.fsync(file.fileno())
    return digest.hexdigest()


def _video(kind: str, head: bytes | bytearray) -> bool:
    """视频响应；通用二进制响应看文件头是不是 MP4。"""
    if kind.startswith("video/"):
        return True
    return kind in GENERIC_MEDIA and head[4:8] == b"ftyp"


async def trusted(request: httpx.Request) -> None:
    """每一跳都要是 https 和受信任的媒体域名。"""
    host = request.url.host
    if request.url.scheme != "https" or not any(
        host == name or host.endswith(f".{name}") for name in MEDIA_HOSTS
    ):
        raise Skip(f"不受信任的媒体地址：{host}")
