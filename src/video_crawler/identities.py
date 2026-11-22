"""身份池，见爬取方案第 3 节。

一个身份就是一个真 Chrome 的资料目录，用它预热出 Cookie 和 tt_chain_token。身份不绑线路，
每次用都向调度中心要一条采集线路。Cookie 和 token 只放内存，重启后按已有目录重新预热。
按设备的预热并发先补足可用身份，再提前刷新快过期的。
"""

import asyncio
import logging
import shutil
import time
from collections import Counter, deque
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from patchright.async_api import Error as BrowserError

from video_crawler import tiktok
from video_crawler.browser import Browsers, Session, session_of
from video_crawler.errors import Retry, Wait
from video_crawler.scheduler import CRAWL, Scheduler
from video_crawler.settings import Settings
from video_crawler.store import Store

log = logging.getLogger(__name__)

TOKEN_TTL = 30 * 60
"""token 按 30 分钟过期算，实际寿命待实测。"""
REFRESH_AFTER = 25 * 60
CHECK_EVERY = 12
WARM_TIMEOUT = 45_000
WARM_RETRY_AFTER = 60
"""同一资料预热失败后稍后再试，让其他资料先用预热名额。"""
COUNTED_FAILS = 3
"""连续失败到这个数就不计入可用身份，后台先补一个新的。"""
MAX_FACTOR = 2
"""身份最多保留目标的两倍：不计入的还占着位置，到第 6 次失败退休后再补。"""
RETIRE_FAILS = 6
COOLDOWN, MAX_COOLDOWN = 60, 30 * 60
RECENT = 3600
"""账号池页面的比率按最近一小时算。"""
RETIRED_SINCE, RETIRED_SHOWN = 24 * 3600, 20
"""快照只带最近 24 小时退休的，最多 20 个。"""


@dataclass
class Identity:
    id: str
    fails: int = 0
    """连续失败次数，只有下载被拒（403）才算身份的失败。"""
    cooldown_until: float = 0
    last_used_at: float = 0
    warmed_at: float = 0
    browser: str | None = None
    retired_at: float | None = None
    retire_reason: str | None = None
    session: Session | None = None
    busy: bool = False
    warm_retry_at: float = 0

    def record(self) -> dict:
        return {
            "id": self.id,
            "fails": self.fails,
            "cooldown_until": self.cooldown_until,
            "last_used_at": self.last_used_at,
            "warmed_at": self.warmed_at,
            "browser": self.browser,
            "retired_at": self.retired_at,
            "retire_reason": self.retire_reason,
        }

    def fresh(self, now: float, seconds: float = TOKEN_TTL) -> bool:
        return bool(self.session and self.session.token) and now - self.warmed_at < seconds


class IdentityPool:
    def __init__(
        self, settings: Settings, store: Store, scheduler: Scheduler, browsers: Browsers
    ) -> None:
        self._root = settings.profile_root
        self._store, self._scheduler, self._browsers = store, scheduler, browsers
        self.target = 0
        """要保持的可用身份数，账号池页面设置，调度中心随心跳按设备分摊下发；拿到之前不新建。"""
        self.warm_slots = 0
        """预热并发随心跳下发，第一次心跳之前暂不开始。"""
        self.browser: str | None = None
        """本机 Chrome 主版本，第一次预热后得知。"""
        self.identities: dict[str, Identity] = {}
        self._warming: dict[str, Identity] = {}
        self._wake = asyncio.Event()
        self._avoid: list[str] = []
        """上次预热失败的线路，下次避开。"""
        self._recent: deque[tuple[float, str, bool]] = deque()
        self._short_since: float | None = None
        """可用身份从什么时候开始少于目标，每次心跳取快照时更新。"""
        for record in store.identities():
            identity = Identity(**record)
            self.identities[identity.id] = identity
            if not self.profile(identity).is_dir():
                self.retire(identity, "资料目录不见了")

    def profile(self, identity: Identity) -> Path:
        return self._root / identity.id

    def summary(self) -> dict:
        """可用：有没过期的 token、不在冷却，正在用的也算。"""
        now = time.time()
        states = Counter(self._state(identity, now) for identity in self.identities.values())
        return {
            "target": self.target,
            "ready": states["ready"] + states["in_use"],
            "in_use": states["in_use"],
            "warming": len(self._warming),
            "cooling": states["cooling"],
            "total": len(self.identities),
        }

    def snapshot(self) -> dict:
        """随心跳报给调度中心，供账号池页面显示；不含 Cookie 和 token。见爬取方案第 6.3 节。"""
        now = time.time()
        while self._recent and self._recent[0][0] < now - RECENT:
            self._recent.popleft()
        summary = self.summary()
        short = summary["ready"] < self.target
        self._short_since = (self._short_since or now) if short else None
        total = Counter(name for _, name, _ in self._recent)
        failed = Counter(name for _, name, ok in self._recent if not ok)
        shown = list(self.identities.values())
        shown.extend(
            identity for key, identity in self._warming.items() if key not in self.identities
        )
        retired = self._store.retired(now - RETIRED_SINCE, RETIRED_SHOWN)
        shown += [Identity(**record) for record in retired]
        return {
            **summary,
            "short_since": self._short_since,
            "warm_total": total["warm"],
            "warm_failed": failed["warm"],
            "list_total": total["list"],
            "list_empty": failed["list"],
            "download_total": total["download"],
            "download_blocked": failed["download"],
            "items": [self._item(identity, now) for identity in shown],
        }

    def note(self, name: str, ok: bool) -> None:
        """记一次结果：warm 预热、list 作品列表请求（空内容算失败）、download 下载（403 算失败）"""
        self._recent.append((time.time(), name, ok))

    def _state(self, identity: Identity, now: float) -> str:
        if identity.retired_at:
            return "retired"
        if identity.id in self._warming:
            return "warming"
        if identity.busy:
            return "in_use"
        if identity.cooldown_until > now:
            return "cooling"
        return "ready" if identity.fresh(now) else "stale"

    def _item(self, identity: Identity, now: float) -> dict:
        record = identity.record()
        return {
            **record,
            **{key: record[key] or None for key in ("cooldown_until", "last_used_at", "warmed_at")},
            "state": self._state(identity, now),
            "token_expires_at": identity.warmed_at + TOKEN_TTL if identity.fresh(now) else None,
        }

    # ---- 用身份 ----

    def _usable_session(self, identity: Identity, now: float) -> bool:
        return (
            identity.cooldown_until <= now
            and identity.browser == self.browser
            and identity.fresh(now)
        )

    def take(self) -> Identity:
        """挑一个能用的身份：连续失败少的优先，再挑最久没用的。挑不到就等补上。"""
        now = time.time()
        ready = [
            identity
            for identity in self.identities.values()
            if not identity.busy and self._usable_session(identity, now)
        ]
        if not ready:
            raise Wait(10, "没有可用身份")
        identity = min(ready, key=lambda item: (item.fails, item.last_used_at))
        identity.busy = True
        return identity

    def give_back(self, identity: Identity, result: str = "neutral") -> None:
        """用完归还。ok：成功，连续失败清零；blocked：下载被拒，冷却并作废 token；
        neutral：结果不怪身份（出口问题、内容本身的结论）。"""
        now = time.time()
        identity.busy, identity.last_used_at = False, now
        if result == "ok":
            identity.fails = 0
        elif result == "blocked":
            identity.fails += 1
            identity.cooldown_until = now + min(COOLDOWN * 2 ** (identity.fails - 1), MAX_COOLDOWN)
            identity.session = None
            if identity.fails >= RETIRE_FAILS:
                return self.retire(identity, f"下载连续 {identity.fails} 次被拒")
        self._store.save_identity(identity.record())
        if not identity.fresh(now, REFRESH_AFTER):
            self._wake.set()

    def refreshed(self, identity: Identity, session: Session) -> None:
        """用浏览器时顺带拿到了新的 Cookie。"""
        if session.token:
            identity.session, identity.warmed_at = session, time.time()
            identity.warm_retry_at = 0

    def refresh(self, identity: Identity) -> None:
        """账号池页面要求立即刷新：当作过期，后台循环下一个就预热它。"""
        identity.warmed_at = identity.warm_retry_at = 0
        self._wake.set()

    # ---- 后台补充和刷新 ----

    def configure(self, target: int, warm_slots: int) -> None:
        if (target, warm_slots) != (self.target, self.warm_slots):
            self.target, self.warm_slots = target, warm_slots
            self._wake.set()

    async def run(self) -> None:
        jobs: set[asyncio.Task[None]] = set()

        def finished(job: asyncio.Task[None]) -> None:
            jobs.discard(job)
            self._wake.set()

        try:
            async with asyncio.TaskGroup() as group:
                while True:
                    self._wake.clear()
                    for _ in range(self.warm_slots - len(jobs)):
                        identity = self._next()
                        if identity is None:
                            break
                        identity.busy = True
                        self._warming[identity.id] = identity
                        job = group.create_task(self._attempt(identity))
                        jobs.add(job)
                        job.add_done_callback(finished)
                    with suppress(TimeoutError):
                        await asyncio.wait_for(self._wake.wait(), CHECK_EVERY)
        finally:
            # 关闭时也释放已预留但还未来得及启动的身份。
            for identity in self._warming.values():
                identity.busy = False
            self._warming.clear()

    async def _attempt(self, identity: Identity) -> None:
        retry_delay = CHECK_EVERY
        retry_profile = True
        try:
            if await self._warm(identity):
                return
        except Wait as wait:
            log.info("预热等线路：%s", wait.reason)
            retry_delay = max(retry_delay, wait.seconds)
            retry_profile = False
        except Exception:
            log.exception("身份 %s 预热异常", identity.id)
        finally:
            identity.busy = False
            self._warming.pop(identity.id)
            if identity.id not in self.identities:
                shutil.rmtree(self.profile(identity), ignore_errors=True)
        if retry_profile:
            identity.warm_retry_at = time.time() + WARM_RETRY_AFTER
        await asyncio.sleep(retry_delay)

    def _next(self) -> Identity | None:
        """先恢复缺少的可用身份，再提前刷新还有效的会话。"""
        if not self.target:
            return None
        now = time.time()
        available = sum(
            identity.fails < COUNTED_FAILS and self._usable_session(identity, now)
            for identity in self.identities.values()
        )
        candidates = [
            identity
            for identity in self.identities.values()
            if not identity.busy
            and identity.cooldown_until <= now
            and identity.warm_retry_at <= now
        ]
        if available >= self.target:
            return min(
                (identity for identity in candidates if not identity.fresh(now, REFRESH_AFTER)),
                key=lambda identity: identity.warmed_at,
                default=None,
            )
        # 正在刷新但仍有有效会话的身份已计在 available 里。
        pending = sum(
            identity.fails < COUNTED_FAILS and not self._usable_session(identity, now)
            for identity in self._warming.values()
        )
        if available + pending >= self.target:
            return None
        stale = min(
            (identity for identity in candidates if not self._usable_session(identity, now)),
            key=lambda identity: (identity.fails, identity.warmed_at),
            default=None,
        )
        if stale is not None and stale.fails < COUNTED_FAILS:
            return stale
        pending_new = sum(key not in self.identities for key in self._warming)
        if len(self.identities) + pending_new < self.target * MAX_FACTOR:
            return Identity(uuid4().hex[:12])
        return stale

    async def _warm(self, identity: Identity) -> bool:
        """预热：打开首页等页面数据出现，拿到 tt_chain_token 才算成功。新建的成功后才加入身份池。"""
        lease = await self._scheduler.lease(CRAWL, avoid=self._avoid)
        session, detail = None, "预热没拿到 tt_chain_token"
        try:
            async with self._browsers.open(
                self.profile(identity), lease.route, warming=True
            ) as page:
                await page.goto(tiktok.HOME, wait_until="domcontentloaded", timeout=WARM_TIMEOUT)
                await page.wait_for_selector(
                    f"#{tiktok.DATA_ID}", state="attached", timeout=WARM_TIMEOUT
                )
                session = await session_of(page)
        except (BrowserError, Retry) as error:
            detail = f"预热失败：{error}"
        finally:
            ok = bool(session and session.token)
            self.note("warm", ok)
            await self._scheduler.release(
                lease, "normal" if ok else "suspect", None if ok else detail
            )
        if not ok:
            self._avoid = [lease.line]
            log.info("身份 %s %s", identity.id, detail)
            return False
        self._avoid = []
        log.info("身份 %s 预热好了", identity.id)
        self.browser = identity.browser = session.browser
        self.refreshed(identity, session)
        self._store.save_identity(identity.record())
        self.identities[identity.id] = identity
        for other in list(self.identities.values()):
            if other.browser != self.browser and not other.busy:
                self.retire(other, f"Chrome 从 {other.browser} 换成了 {self.browser}")
        return True

    def retire(self, identity: Identity, reason: str) -> None:
        """删掉资料目录，后台会补一个新的。"""
        identity.retired_at, identity.retire_reason = time.time(), reason
        self.identities.pop(identity.id, None)
        shutil.rmtree(self.profile(identity), ignore_errors=True)
        self._store.save_identity(identity.record())
        log.info("身份 %s 退休：%s", identity.id, reason)
        self._wake.set()
