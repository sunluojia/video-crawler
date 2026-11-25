"""按空闲的执行名额和身份向调度中心领任务，内存只保留正在执行的尝试。"""

import asyncio
import contextlib
import logging
import random
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime
from functools import partial
from uuid import uuid4

import httpx
from redis.exceptions import RedisError

from video_crawler.errors import Gone, Retry, Wait
from video_crawler.scheduler import DOWNLOAD, ClaimedTask, Scheduler

log = logging.getLogger(__name__)

CRAWL_CREATOR, DOWNLOAD_VIDEO = "crawl_creator", "download_video"
MAX_ATTEMPTS = 3
BACKOFF = (60, 300)
LIMITS = {CRAWL_CREATOR: 120, DOWNLOAD_VIDEO: 600}
PAUSE_AFTER, MAX_PAUSE = 5, 600
CLAIM_RETRY_DELAY, MAX_CLAIM_PAUSE = 5, 15
type Jobs = dict[str, tuple[ClaimedTask, asyncio.Task[None]]]


class Runner:
    def __init__(
        self,
        scheduler: Scheduler,
        attempts: dict[str, Callable[[ClaimedTask], Awaitable[dict | None]]],
        slots: dict[str, int],
        *,
        available_identities: Callable[[], int],
        publish: Callable[[dict], Awaitable[None]],
    ) -> None:
        self._scheduler, self._attempts, self._publish = scheduler, attempts, publish
        self._available_identities = available_identities
        self.slots = slots.copy()
        """每类并发由心跳下发，调小时进行中的做完为止。"""
        self.running: Jobs = {}
        self._returning: Jobs = {}
        self._stopping: set[asyncio.Task] = set()
        self.paused_batches: set[str] = set()
        self._claim_controls: set[tuple[str, str]] | None = None
        self._claim_pause_until: dict[str, float] = {}
        self._preferred = CRAWL_CREATOR
        self._streak = 0
        self.paused_until = 0.0
        self._wake = asyncio.Event()

    def wake(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        while True:
            self._wake.clear()
            try:
                await self._fill()
            except httpx.HTTPError as error:
                self._pause_claims(CLAIM_RETRY_DELAY)
                log.warning("认领任务失败，稍后重试：%s", error)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), 1)

    def busy(self, kind: str) -> int:
        return sum(task.kind == kind for task, _ in self.running.values())

    def attempt_ids(self) -> set[str]:
        return {
            attempt_id
            for jobs in (self.running, self._returning)
            for attempt_id, (_, job) in jobs.items()
            if not job.done() and job not in self._stopping
        }

    def jobs(self) -> list[asyncio.Task[None]]:
        return [job for jobs in (self.running, self._returning) for _, job in jobs.values()]

    def stop(self) -> list[asyncio.Task[None]]:
        jobs = self.jobs()
        for job in jobs:
            self._stop(job)
        return jobs

    def _capacity(self) -> dict[str, int]:
        downloads = self.busy(DOWNLOAD_VIDEO)
        download_slots = self.slots.get(DOWNLOAD_VIDEO, 0) - downloads
        if self.paused_until > time.time():
            download_slots = 0
        elif self._streak >= PAUSE_AFTER:
            download_slots = min(download_slots, 1 - downloads)
        return {
            CRAWL_CREATOR: max(self.slots.get(CRAWL_CREATOR, 0) - self.busy(CRAWL_CREATOR), 0),
            DOWNLOAD_VIDEO: max(download_slots, 0),
        }

    def _claim_capacity(self) -> dict[str, int]:
        capacity = self._capacity()
        now = time.monotonic()
        for kind, until in self._claim_pause_until.items():
            if until > now:
                capacity[kind] = 0
        return capacity

    def _pause_claims(self, retry_after: float, use: str | None = None) -> None:
        until = time.monotonic() + min(retry_after, MAX_CLAIM_PAUSE)
        # 下载取链也用采集线路，身份池和调度中心则由两类任务共用。
        kinds = (DOWNLOAD_VIDEO,) if use == DOWNLOAD else (CRAWL_CREATOR, DOWNLOAD_VIDEO)
        for kind in kinds:
            self._claim_pause_until[kind] = max(until, self._claim_pause_until.get(kind, until))

    @staticmethod
    def _allocate(
        capacity: dict[str, int], available: int, order: tuple[str, str]
    ) -> dict[str, int]:
        first, second = order
        request = {
            first: min(capacity.get(first, 0), (available + 1) // 2),
            second: min(capacity.get(second, 0), available // 2),
        }
        spare = available - sum(request.values())
        for kind in order:
            extra = min(spare, capacity.get(kind, 0) - request[kind])
            request[kind] += extra
            spare -= extra
        return {kind: count for kind, count in request.items() if count}

    async def _fill(self) -> None:
        available = self._available_identities()
        if available <= 0:
            return
        capacity = self._claim_capacity()
        other = DOWNLOAD_VIDEO if self._preferred == CRAWL_CREATOR else CRAWL_CREATOR
        order = (self._preferred, other)
        request = self._allocate(capacity, available, order)
        if not request:
            return
        if all(capacity.values()):
            self._preferred = other
        claimed = await self._claim(request)
        spare = min(available - claimed.total(), self._available_identities())
        if spare > 0:
            # 某类没领满就把余量给另一类，单身份时首选为空也不用等下次轮询。
            capacity = {
                kind: slots
                for kind, slots in self._claim_capacity().items()
                if claimed[kind] >= request.get(kind, 0)
            }
            if request := self._allocate(capacity, spare, order):
                await self._claim(request)

    async def _claim(self, kinds: dict[str, int]) -> Counter[str]:
        controls: set[tuple[str, str]] = set()
        self._claim_controls = controls
        try:
            claimed = await self._scheduler.claim(kinds)
        finally:
            self._claim_controls = None
        capacity, available = self._capacity(), self._available_identities()
        for task in claimed:
            if task.attempt_id in self.running or task.attempt_id in self._returning:
                continue
            if ("batch", task.batch_id) in controls or ("task", task.task_id) in controls:
                continue
            if task.kind == CRAWL_CREATOR:
                if ("pause", task.batch_id) in controls:
                    self._return(task, "批次已暂停")
                    continue
                # 调度中心允许新认领时，以其批次状态为准，补上可能漏收的恢复通知。
                self.paused_batches.discard(task.batch_id)
            if not capacity.get(task.kind, 0) or available <= 0:
                if available <= 0:
                    self._pause_claims(CLAIM_RETRY_DELAY)
                self._return(task, "本机身份或执行名额已变更")
                continue
            for previous, job in self.running.values():
                if previous.task_id == task.task_id:
                    self._stop(job)
            job = asyncio.create_task(self.attempt(task))
            self.running[task.attempt_id] = task, job
            job.add_done_callback(partial(self._finished, self.running, task))
            capacity[task.kind] -= 1
            available -= 1
        return Counter(task.kind for task in claimed)

    def _return(self, task: ClaimedTask, reason: str) -> None:
        job = asyncio.create_task(self._defer(task, 1, reason))
        self._returning[task.attempt_id] = task, job
        job.add_done_callback(partial(self._finished, self._returning, task))

    def _finished(self, jobs: Jobs, task: ClaimedTask, job: asyncio.Task[None]) -> None:
        jobs.pop(task.attempt_id, None)
        self._stopping.discard(job)
        self.wake()
        if not job.cancelled() and (error := job.exception()):
            log.error("尝试 %s 收尾失败，等待调度中心回收", task.attempt_id, exc_info=error)

    def _stop(self, job: asyncio.Task[None]) -> bool:
        if job.done() or job in self._stopping:
            return False
        self._stopping.add(job)
        # 超时清理或另一条撤销已在进行时，避免第二次取消打断资源清理。
        if not job.cancelling():
            job.cancel()
        return True

    def revoke(self, attempt_ids: Iterable[str]) -> None:
        for attempt_id in attempt_ids:
            for jobs in (self.running, self._returning):
                if attempt_id in jobs:
                    self._stop(jobs[attempt_id][1])

    def pause(self, batch_id: str) -> None:
        self.paused_batches.add(batch_id)
        if self._claim_controls is not None:
            self._claim_controls.add(("pause", batch_id))
        for task, job in self.running.values():
            if task.batch_id == batch_id and task.kind == CRAWL_CREATOR and self._stop(job):
                self._return(task, "批次已暂停")

    def resume(self, batch_id: str) -> None:
        self.paused_batches.discard(batch_id)
        if self._claim_controls is not None:
            self._claim_controls.discard(("pause", batch_id))
        self.wake()

    def cancel(self, *, batch_id: str | None = None, task_id: str | None = None) -> None:
        if batch_id is not None:
            self.paused_batches.discard(batch_id)
        if self._claim_controls is not None:
            if batch_id is not None:
                self._claim_controls.add(("batch", batch_id))
            if task_id is not None:
                self._claim_controls.add(("task", task_id))
        for jobs in (self.running, self._returning):
            for task, job in jobs.values():
                if task.batch_id == batch_id or task.task_id == task_id:
                    self._stop(job)

    async def attempt(self, task: ClaimedTask) -> None:
        async with asyncio.TaskGroup() as group:
            group.create_task(self._accept(task))
            result = await self._execute(task)
        if result is not None:
            await self._publish_result(result)

    async def _execute(self, task: ClaimedTask) -> dict | None:
        try:
            async with asyncio.timeout(LIMITS[task.kind]):
                fields = await self._attempts[task.kind](task)
        except Exception as error:
            if asyncio.current_task() in self._stopping:
                raise asyncio.CancelledError from error
            if isinstance(error, Wait):
                retry_after = max(error.seconds, 1)
                self._pause_claims(retry_after, error.use)
                await self._defer(task, retry_after, error.reason)
                return None
            if isinstance(error, Gone):
                log.info("%s %s 结束：%s", task.kind, task.task_id, error)
                return self._failure_event(task, str(error))
            return await self._retry(task, error)
        if asyncio.current_task() in self._stopping:
            raise asyncio.CancelledError
        log.info("%s %s 完成", task.kind, task.task_id)
        self._count(task, ok=True)
        return self._event(task, "crawled", **fields) if fields is not None else None

    async def _retry(self, task: ClaimedTask, error: Exception) -> dict | None:
        line = None
        if isinstance(error, Retry):
            reason, line = error.reason, error.line
        elif isinstance(error, TimeoutError):
            reason = f"一次尝试超过 {LIMITS[task.kind]} 秒"
        else:
            log.exception("任务 %s 出错", task.task_id)
            reason = f"程序出错：{error!r}"
        attempts = task.attempts + 1
        log.warning("%s %s 第 %s 次没成：%s", task.kind, task.task_id, attempts, reason)
        self._count(task, ok=False)
        if attempts >= MAX_ATTEMPTS:
            profile = error.profile if isinstance(error, Retry) else None
            summary = "；".join([*task.history, f"第 {attempts} 次：{reason}"])
            return self._failure_event(task, summary, profile)
        delay = BACKOFF[attempts - 1] * random.uniform(0.8, 1.2)
        await self._defer(task, delay, reason, consume_attempt=True, avoid_line=line)
        return None

    async def _defer(
        self,
        task: ClaimedTask,
        seconds: float,
        reason: str,
        *,
        consume_attempt: bool = False,
        avoid_line: str | None = None,
    ) -> None:
        delay = 1
        while True:
            try:
                await self._scheduler.defer(
                    task.attempt_id, seconds, reason, consume_attempt, avoid_line
                )
            except httpx.HTTPError as error:
                if isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 409:
                    # 已退回、撤销或到期。
                    return
                log.warning("尝试 %s 退回失败，稍后重试：%s", task.attempt_id, error)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 15)
            else:
                return

    async def _accept(self, task: ClaimedTask) -> None:
        # 调度中心认领时已记录接单，这条事件保留给旧的事件消费者。
        try:
            await self._publish(self._event(task, "accepted"))
        except RedisError as error:
            log.warning("尝试 %s 的 accepted 事件未能投递：%s", task.attempt_id, error)

    async def _publish_result(self, item: dict) -> None:
        # Redis 确认入队前继续为尝试续租，重发使用同一个事件 ID。
        delay = 1
        while True:
            try:
                await self._publish(item)
            except RedisError as error:
                log.warning("尝试 %s 的结果投递失败，稍后重试：%s", item["attempt_id"], error)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 15)
            else:
                return

    def _event(self, task: ClaimedTask, type_: str, **fields) -> dict:
        return {
            "event_id": str(uuid4()),
            "task_id": task.task_id,
            "run": task.run,
            "type": type_,
            "occurred_at": datetime.now(UTC).isoformat(),
            "executor_id": self._scheduler.instance,
            "executor_session_id": str(self._scheduler.session_id),
            "attempt_id": task.attempt_id,
            **fields,
        }

    def _failure_event(self, task: ClaimedTask, reason: str, profile: dict | None = None) -> dict:
        if task.kind == CRAWL_CREATOR:
            return self._event(task, "crawled", conclusion="failed", reason=reason, profile=profile)
        return self._event(task, "download_failed", reason=reason)

    def _count(self, task: ClaimedTask, ok: bool) -> None:
        if task.kind != DOWNLOAD_VIDEO:
            return
        self._streak = 0 if ok else self._streak + 1
        if self._streak >= PAUSE_AFTER:
            self.paused_until = time.time() + min(60 * 2 ** (self._streak - PAUSE_AFTER), MAX_PAUSE)
            log.warning(
                "下载连续 %s 次没成，设备暂停到 %s", self._streak, time.ctime(self.paused_until)
            )
