"""本地任务队列：按名额开始一次次尝试。见单视频下载方案第 1、3、4 节和爬取方案第 7 节。

- 名额只在尝试进行中占用；等重试、等线路、等身份都不占。
- 这次没做成：算一次尝试，1 分钟、5 分钟后避开失败过的线路再试，3 次都没成就回传失败。
- 环境问题：按给出的时间再排，不算次数。
- 有下载在等采集线路时，先不开始新的获取作品。
- 下载连续 5 次没成，设备自己暂停，60 秒起每次翻倍，最长 10 分钟；暂停结束先放一条试探。
"""

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import uuid4

from video_crawler.errors import Gone, Retry, Wait
from video_crawler.scheduler import CRAWL
from video_crawler.store import Store, Task

log = logging.getLogger(__name__)

CRAWL_CREATOR, DOWNLOAD_VIDEO = "crawl_creator", "download_video"
MAX_ATTEMPTS = 3
BACKOFF = (60, 300)
LIMITS = {CRAWL_CREATOR: 120, DOWNLOAD_VIDEO: 600}
"""一次尝试最长多少秒。"""
PAUSE_AFTER, MAX_PAUSE = 5, 600


def event(task_id: str, run: int, type_: str, **fields) -> dict:
    return {
        "event_id": str(uuid4()),
        "task_id": task_id,
        "run": run,
        "type": type_,
        "occurred_at": datetime.now(UTC).isoformat(),
        **fields,
    }


def failed(task: Task, reason: str, profile: dict | None = None) -> dict:
    if task.kind == CRAWL_CREATOR:
        return event(
            task.task_id, task.run, "crawled", conclusion="failed", reason=reason, profile=profile
        )
    return event(task.task_id, task.run, "download_failed", reason=reason)


class Runner:
    def __init__(
        self,
        store: Store,
        attempts: dict[str, Callable[[Task], Awaitable[dict | None]]],
        slots: dict[str, int],
    ) -> None:
        self._store, self._attempts = store, attempts
        self.slots = slots
        """每类同时做几个，调度中心随心跳下发；调小时进行中的做完为止。"""
        self.running: dict[str, tuple[Task, asyncio.Task]] = {}
        self.starved: dict[str, str] = {}
        """在等采集线路的下载：任务 ID → 批次 ID。"""
        self._streak = 0
        """下载连续没成的次数。"""
        self.paused_until = 0.0
        self._wake = asyncio.Event()

    def wake(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        while True:
            now = time.time()
            for kind in (DOWNLOAD_VIDEO, CRAWL_CREATOR):
                while self._can_start(kind, now) and (
                    task := self._store.due(kind, now, set(self.running))
                ):
                    self.running[task.task_id] = (task, asyncio.create_task(self.attempt(task)))
            self._wake.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), 1)

    def busy(self, kind: str) -> int:
        return sum(task.kind == kind for task, _ in self.running.values())

    def _can_start(self, kind: str, now: float) -> bool:
        busy = self.busy(kind)
        if busy >= self.slots[kind]:
            return False
        if kind == CRAWL_CREATOR:
            return not self.starved
        return self.paused_until <= now and not (self._streak >= PAUSE_AFTER and busy)

    def cancel(self, batch_id: str | None = None, task_id: str | None = None) -> None:
        """取消进行中的尝试：关闭连接和浏览器，归还线路，清掉半截文件。"""
        for task, running in list(self.running.values()):
            if task.batch_id == batch_id or task.task_id == task_id:
                running.cancel()
        self.starved.pop(task_id, None)
        self.forget(batch_id)

    def forget(self, batch_id: str | None) -> None:
        """批次取消后，它的下载不再算在等采集线路，免得挡住其他批次的获取作品。"""
        for waiting, batch in list(self.starved.items()):
            if batch == batch_id:
                del self.starved[waiting]

    async def attempt(self, task: Task) -> None:
        starved = False
        try:
            async with asyncio.timeout(LIMITS[task.kind]):
                fields = await self._attempts[task.kind](task)
        except Wait as wait:
            task.next_at, task.waiting = time.time() + wait.seconds, wait.reason
            starved = task.kind == DOWNLOAD_VIDEO and wait.use == CRAWL
            self._store.save(task)
        except Gone as gone:
            log.info("%s %s 结束：%s", task.kind, task.task_id, gone)
            self._store.finish(task, failed(task, str(gone)))
        except Exception as error:  # 包括 Retry、超时和意料之外的错误，都算这次没做成
            self._retry(task, error)
        else:
            log.info("%s %s 完成", task.kind, task.task_id)
            self._store.finish(task, fields and event(task.task_id, task.run, "crawled", **fields))
            self._count(task, ok=True)
        finally:
            if starved:
                self.starved[task.task_id] = task.batch_id
            else:
                self.starved.pop(task.task_id, None)
            self.running.pop(task.task_id, None)
            self.wake()

    def _retry(self, task: Task, error: Exception) -> None:
        if isinstance(error, Retry):
            reason = error.reason
            if error.line and error.line not in task.avoid:
                task.avoid.append(error.line)
        elif isinstance(error, TimeoutError):
            reason = f"一次尝试超过 {LIMITS[task.kind]} 秒"
        else:
            log.exception("任务 %s 出错", task.task_id)
            reason = f"程序出错：{error!r}"
        task.attempts += 1
        task.history.append(f"第 {task.attempts} 次：{reason}")
        log.warning("%s %s 第 %s 次没成：%s", task.kind, task.task_id, task.attempts, reason)
        self._count(task, ok=False)
        if task.attempts >= MAX_ATTEMPTS:
            profile = error.profile if isinstance(error, Retry) else None
            self._store.finish(task, failed(task, "；".join(task.history), profile))
            return
        delay = BACKOFF[task.attempts - 1] * random.uniform(0.8, 1.2)
        task.next_at, task.waiting = time.time() + delay, reason
        self._store.save(task)

    def _count(self, task: Task, ok: bool) -> None:
        if task.kind != DOWNLOAD_VIDEO:
            return
        self._streak = 0 if ok else self._streak + 1
        if self._streak >= PAUSE_AFTER:
            self.paused_until = time.time() + min(60 * 2 ** (self._streak - PAUSE_AFTER), MAX_PAUSE)
            log.warning(
                "下载连续 %s 次没成，设备暂停到 %s", self._streak, time.ctime(self.paused_until)
            )
