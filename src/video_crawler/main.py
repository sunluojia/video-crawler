"""启动爬取程序：一个进程、一个事件循环。

- Dramatiq 线程只收任务：记进本地状态表并排好 accepted 事件，然后唤醒任务队列。
- 事件循环里跑任务队列、身份池的后台循环、待发事件的投递、心跳和控制接口。
"""

import asyncio
import contextlib
import logging
from importlib.metadata import version

import dramatiq
import httpx
import uvicorn
from dramatiq import Message
from dramatiq.brokers.redis import RedisBroker

from video_crawler.api import control_api
from video_crawler.browser import Browsers
from video_crawler.crawl import Crawler
from video_crawler.download import Downloader
from video_crawler.identities import IdentityPool
from video_crawler.runner import CRAWL_CREATOR, DOWNLOAD_VIDEO, Runner, event
from video_crawler.scheduler import Scheduler
from video_crawler.settings import Settings
from video_crawler.store import Store

log = logging.getLogger("video_crawler")

TASKS, EVENTS, EVENT_ACTOR = "crawler.tasks", "crawler.events", "crawler_event"
VERSION = version("video-crawler")


async def serve(settings: Settings) -> None:
    store = Store(settings.data_dir / "state.db")
    scheduler = Scheduler(settings)
    browsers = Browsers(settings.chrome_channel, warm_headless=settings.warm_headless)
    pool = IdentityPool(settings, store, scheduler, browsers)
    runner = Runner(
        store,
        {
            CRAWL_CREATOR: Crawler(pool, scheduler, browsers),
            DOWNLOAD_VIDEO: Downloader(pool, scheduler, browsers, settings.data_dir / "videos"),
        },
        {CRAWL_CREATOR: 0, DOWNLOAD_VIDEO: 0},  # 第一次心跳拿到并发数之前不开始
    )
    broker = RedisBroker(url=settings.redis_url)
    broker.declare_queue(EVENTS)  # 事件发给调度中心，本程序只消费任务队列
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(ignore_resets)

    def receiver(kind: str):
        def receive(body: dict) -> None:
            accepted = event(body["task_id"], body["run"], "accepted")
            if store.receive(kind, body, accepted):
                loop.call_soon_threadsafe(runner.wake)

        return receive

    for kind in (CRAWL_CREATOR, DOWNLOAD_VIDEO):
        dramatiq.actor(receiver(kind), broker=broker, actor_name=kind, queue_name=TASKS)
    worker = dramatiq.Worker(broker, queues={TASKS}, worker_threads=2)

    async def deliver() -> None:
        """投递待发事件；Redis 暂时不通就下次再投，事件 ID 不变，调度中心按它去重。"""
        while True:
            for item in store.events():
                try:
                    message = Message(EVENTS, EVENT_ACTOR, (item,), {}, {})
                    await asyncio.to_thread(broker.enqueue, message)
                except Exception as error:
                    log.warning("投递事件失败，稍后重试：%s", error)
                    break
                store.sent(item["event_id"])
            await asyncio.sleep(1)

    async def heartbeat() -> None:
        """注册兼心跳；回复里是页面上设置的身份数和并发，改了不用重启。"""
        while True:
            try:
                reply = await scheduler.register(settings.url, VERSION, pool.snapshot())
                pool.target = reply["identity_target"]
                slots = {
                    CRAWL_CREATOR: reply["crawl_slots"],
                    DOWNLOAD_VIDEO: reply["download_slots"],
                }
                if slots != runner.slots:
                    log.info("并发：同时获取作品 %s 个，同时下载 %s 个", *slots.values())
                    runner.slots.update(slots)
                    await browsers.resize(reply["crawl_slots"] + 1)
                    runner.wake()
            except httpx.HTTPError as error:
                log.warning("注册失败，稍后重试：%s", error)
            await asyncio.sleep(15)

    token = settings.service_token.get_secret_value()
    server = uvicorn.Server(
        uvicorn.Config(
            control_api(token, store, runner, pool, scheduler),
            host=settings.host,
            port=settings.port,
        )
    )
    await browsers.start()
    worker.start()
    background = [
        asyncio.create_task(job) for job in (runner.run(), pool.run(), deliver(), heartbeat())
    ]
    try:
        await server.serve()
    finally:
        # 进行中的尝试直接中止：归还线路、清掉半截文件，任务留在本地状态表里，下次启动接着做
        worker.stop()
        jobs = background + [job for _, job in runner.running.values()]
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        await browsers.stop()
        with contextlib.suppress(httpx.HTTPError):
            await scheduler.withdraw()


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
