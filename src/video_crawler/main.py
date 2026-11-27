"""在同一事件循环里认领任务、维护身份、投递结果并处理心跳和控制请求。"""

import asyncio
import contextlib
import logging
from functools import partial
from importlib.metadata import version

import httpx
import uvicorn
from dramatiq import Message
from dramatiq.brokers.redis import RedisBroker
from redis import Redis
from redis.exceptions import RedisError

from video_crawler import tiktok
from video_crawler.api import control_api
from video_crawler.browser import Browsers
from video_crawler.crawl import Crawler
from video_crawler.download import Downloader
from video_crawler.identities import IdentityPool
from video_crawler.runner import CRAWL_CREATOR, DOWNLOAD_VIDEO, Runner
from video_crawler.scheduler import Scheduler
from video_crawler.settings import Settings
from video_crawler.store import IdentityStore

log = logging.getLogger("video_crawler")

EVENTS, EVENT_ACTOR = "crawler.events", "crawler_event"
VERSION = version("video-crawler")


async def heartbeat(
    scheduler: Scheduler,
    url: str,
    runner: Runner,
    pool: IdentityPool,
    browsers: Browsers,
    *,
    pool_size: int = 0,
) -> None:
    while True:
        try:
            reply = await scheduler.register(url, VERSION, pool.snapshot(), runner.attempt_ids())
            runner.revoke(reply.get("revoked_attempts", []))
            slots = {
                CRAWL_CREATOR: reply["crawl_slots"],
                DOWNLOAD_VIDEO: reply["download_slots"],
            }
            warm_slots = reply.get("warm_slots")
            if warm_slots is None:
                warm_slots = 1
            if slots != runner.slots or warm_slots != pool.warm_slots:
                log.info(
                    "并发：同时获取作品 %s 个，同时下载 %s 个，同时预热 %s 个",
                    slots[CRAWL_CREATOR],
                    slots[DOWNLOAD_VIDEO],
                    warm_slots,
                )
                active_browsers = slots[CRAWL_CREATOR] + warm_slots
                browser_limit = max(active_browsers, pool_size) if active_browsers else 0
                await browsers.resize(browser_limit)
                runner.slots.update(slots)
                runner.wake()
            pool.configure(reply["identity_target"] or 0, warm_slots)
        except httpx.HTTPError as error:
            log.warning("注册失败，稍后重试：%s", error)
        await asyncio.sleep(15)


async def serve(settings: Settings) -> None:
    with Redis.from_url(settings.redis_url, socket_connect_timeout=3, socket_timeout=3) as redis:
        await _serve(settings, redis)


async def publish_event(broker: RedisBroker, item: dict) -> None:
    message = Message(EVENTS, EVENT_ACTOR, (item,), {}, {})
    await asyncio.to_thread(broker.enqueue, message)


async def _serve(settings: Settings, redis: Redis) -> None:
    store = IdentityStore(redis, settings.instance_id, keep_profile_dirs=settings.preserve_profiles)
    scheduler = Scheduler(settings)
    browsers = Browsers(
        settings.chrome_channel,
        warm_headless=settings.warm_headless,
        block_nonessential_resources=settings.block_nonessential_resources,
        blocked_resource_types=settings.blocked_resource_types,
        reuse_browser_context=settings.reuse_browser_context,
        browser_context_max_uses=settings.browser_context_max_uses,
        browser_context_idle_ttl_seconds=settings.browser_context_idle_ttl_seconds,
    )
    pool = IdentityPool(settings, store, scheduler, browsers)
    await pool.restore()
    media_cache = tiktok.MediaCache(redis=redis if settings.shared_media_cache else None)
    crawler = Crawler(
        pool,
        scheduler,
        browsers,
        media_cache=media_cache,
        max_video_height=settings.max_video_height,
    )
    downloader = Downloader(
        pool,
        scheduler,
        browsers,
        settings.data_dir / "videos",
        media_cache=media_cache,
        max_video_height=settings.max_video_height,
        slow_bytes=settings.download_slow_bytes,
        slow_window=settings.download_slow_window,
        range_parts=settings.download_range_parts,
        range_min_bytes=settings.download_range_min_bytes,
    )
    broker = RedisBroker(client=redis)
    broker.declare_queue(EVENTS)
    runner = Runner(
        scheduler,
        {
            CRAWL_CREATOR: crawler,
            DOWNLOAD_VIDEO: downloader,
        },
        {CRAWL_CREATOR: 0, DOWNLOAD_VIDEO: 0},  # 第一次心跳拿到并发数之前不开始
        available_identities=lambda: len(pool.available()),
        available_crawl_identities=lambda: len(pool.available_for_crawl()),
        settings=settings,
        cached_download=downloader.has_cached_media,
        publish=partial(publish_event, broker),
    )
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(ignore_resets)

    token = settings.service_token.get_secret_value()
    server = uvicorn.Server(
        uvicorn.Config(
            control_api(
                token,
                runner,
                pool,
                scheduler,
                downloads=downloader.progress,
                download_speed=downloader.download_speed,
                crawls=crawler.progress,
            ),
            host=settings.host,
            port=settings.port,
        )
    )
    await browsers.start()
    pool_size = settings.browser_context_pool_size if settings.reuse_browser_context else 0
    background = [
        asyncio.create_task(runner.run()),
        asyncio.create_task(pool.run()),
        asyncio.create_task(pool.persist_loop()),
        asyncio.create_task(
            heartbeat(scheduler, settings.url, runner, pool, browsers, pool_size=pool_size)
        ),
    ]

    def stopped(job: asyncio.Task) -> None:
        """后台循环出错停下时整个程序退出：设备显示离线、由重启策略拉起，而不是在线却不干活。"""
        if not job.cancelled() and (error := job.exception()):
            log.critical("后台循环出错停止，程序退出", exc_info=error)
            server.should_exit = True

    for job in background:
        job.add_done_callback(stopped)
    try:
        await server.serve()
    finally:
        # 停止续租，调度中心回收未完成的尝试，已入队的事件继续由 Redis 投递。
        try:
            for job in background:
                job.cancel()
            await asyncio.gather(*background, *runner.stop(), return_exceptions=True)
            await pool.persist()
        except RedisError as error:
            log.warning("退出时身份元数据写入 Redis 失败：%s", error)
        finally:
            await browsers.stop()
        with contextlib.suppress(httpx.HTTPError):
            await scheduler.withdraw()
    if any(not job.cancelled() and job.exception() for job in background):
        raise SystemExit(1)  # 出错退出，重启策略据此拉起


def ignore_resets(loop: asyncio.AbstractEventLoop, context: dict) -> None:
    """对方先断开（RST）的连接，Windows 上 asyncio 关它时会报 ConnectionResetError。

    这是 CPython 的老问题，不影响结果；Chrome 一关，几十条连接一起报，会把日志刷满。其他照常记。
    """
    if not isinstance(context.get("exception"), ConnectionResetError):
        loop.default_exception_handler(context)


def main() -> None:
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # 每个请求一行太多
    asyncio.run(serve(Settings()))
