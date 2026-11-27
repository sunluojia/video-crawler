"""身份池，见爬取方案第 3 节。

一个身份就是一个真 Chrome 的资料目录，用它预热出 Cookie 和 tt_chain_token。身份不绑线路，
每次用都向调度中心要一条采集线路。Cookie 和 token 只放内存，重启后按已有目录重新预热。
按设备的预热并发先补足可用身份，再提前刷新快过期的。
"""

import asyncio
import logging
import re
import shutil
import time
from collections import Counter, deque
from collections.abc import Collection
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import uuid4

from patchright.async_api import Error as BrowserError
from redis.exceptions import RedisError

from video_crawler import tiktok
from video_crawler.browser import Browsers, Session, session_of
from video_crawler.errors import WARM_EMPTY_WAIT, Retry, Wait, crawl_identity_hold
from video_crawler.scheduler import CRAWL, Scheduler
from video_crawler.settings import Settings
from video_crawler.store import RETIRED_SHOWN, RETIRED_SINCE, IdentityStore

log = logging.getLogger(__name__)

TOKEN_TTL = 30 * 60
"""token 按 30 分钟过期算，实际寿命待实测。"""
REFRESH_AFTER = 25 * 60
CHECK_EVERY = 12
WARM_TIMEOUT = 45_000
WARM_RETRY_AFTER = 60
WARM_LOCAL_WAIT = "warm_local_wait"
"""同一资料预热失败后稍后再试，让其他资料先用预热名额。"""
DOWNLOAD_IDENTITY_WAIT = 60
"""待取链的下载等身份的上限，超时退回时不扣尝试。"""
COUNTED_FAILS = 3
"""连续失败到这个数就不计入可用身份，后台先补一个新的。"""
MAX_FACTOR = 2
"""身份最多保留目标的两倍：不计入的还占着位置，到第 6 次失败退休后再补。"""
RETIRE_FAILS = 6
COOLDOWN, MAX_COOLDOWN = 60, 30 * 60
RECENT = 3600
"""账号池页面的比率按最近一小时算。"""
IDENTITY_ID = re.compile(r"[0-9a-f]{12}")
type DownloadCredentials = tuple[str, str]  # User-Agent, tt_chain_token


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
    listing_fails: int = 0
    """当前浏览器会话中以空正文结束的采集次数。重启后会重新预热，所以只记在内存。"""
    listing_refresh_at: float | None = None
    """需要刷新列表会话时的单调时钟截止。有效下载凭据仍可使用。"""

    @property
    def consecutive_failures(self) -> int:
        return max(self.fails, self.listing_fails)

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
        self, settings: Settings, store: IdentityStore, scheduler: Scheduler, browsers: Browsers
    ) -> None:
        self._root = settings.profile_root
        self._store, self._scheduler, self._browsers = store, scheduler, browsers
        self._keep_profile_dirs = settings.preserve_profiles
        self._crawl_policy = settings.crawl_policy_enabled
        self._listing_hold = settings.crawl_empty_session_hold_seconds if self._crawl_policy else 0
        self._warm_recovery = settings.crawl_warm_recovery_min_seconds if self._crawl_policy else 0
        self._last_warm_started: float | None = None
        self._warm_wait_until = self._warm_pressure_until = 0.0
        self._active_warms = 0
        self.target = 0
        """要保持的可用身份数，账号池页面设置，调度中心随心跳按设备分摊下发；拿到之前不新建。"""
        self.warm_slots = 0
        """预热并发随心跳下发，第一次心跳之前暂不开始。"""
        self.browser: str | None = None
        """本机 Chrome 主版本，第一次预热后得知。"""
        self.identities: dict[str, Identity] = {}
        self._warming: dict[str, Identity] = {}
        self._wake = asyncio.Event()
        self._available_changed = asyncio.Event()
        self._avoid: list[str] = []
        """上次预热失败的线路，下次避开。"""
        self._recent: deque[tuple[float, str, bool]] = deque()
        self._short_since: float | None = None
        """可用身份从什么时候开始少于目标，每次心跳取快照时更新。"""
        self._dirty: dict[str, dict] = {}
        self._retired: deque[Identity] = deque(maxlen=RETIRED_SHOWN)

    async def restore(self) -> None:
        """加载本机元数据并重新预热已有的 Chrome 资料目录。"""
        known = set()
        for record in await asyncio.to_thread(self._store.records):
            if not IDENTITY_ID.fullmatch(record["id"]):
                continue
            identity = Identity(**record)
            known.add(identity.id)
            if identity.retired_at is not None:
                self.identities.pop(identity.id, None)
                self._retired.append(identity)
                continue
            self.identities[identity.id] = identity
        for identity in list(self.identities.values()):
            if not self.profile(identity).is_dir():
                self.retire(identity, "资料目录不见了")
        if self._root.is_dir():
            for path in self._root.iterdir():
                if path.name not in known and IDENTITY_ID.fullmatch(path.name) and path.is_dir():
                    self.identities[path.name] = Identity(path.name)

    async def persist(self) -> None:
        pending = self._dirty.copy()
        if not pending:
            return
        write = asyncio.create_task(asyncio.to_thread(self._store.save, pending.values()))
        try:
            await asyncio.shield(write)
        except asyncio.CancelledError:
            # 退出会再刷一次最新状态，先等旧的 Redis 写入完成。
            await asyncio.gather(write, return_exceptions=True)
            raise
        for key, record in pending.items():
            if self._dirty.get(key) is record:
                del self._dirty[key]

    async def persist_loop(self) -> None:
        while True:
            try:
                await self.persist()
            except RedisError as error:
                log.warning("身份元数据写入 Redis 失败，稍后重试：%s", error)
            await asyncio.sleep(1)

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
        shown += [
            item for item in reversed(self._retired) if item.retired_at >= now - RETIRED_SINCE
        ]
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
            "fails": identity.consecutive_failures,
            "listing_fails": identity.listing_fails,
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

    def available(self) -> list[Identity]:
        now = time.time()
        return [
            identity
            for identity in self.identities.values()
            if not identity.busy and self._usable_session(identity, now)
        ]

    def available_for_crawl(self) -> list[Identity]:
        return [identity for identity in self.available() if identity.listing_refresh_at is None]

    def take(
        self,
        *,
        preferred_id: str | None = None,
        preferred_ids: Collection[str] = (),
        for_listing: bool = False,
    ) -> Identity:
        """优先用指定的空闲身份（含同线路热上下文身份），其次按连续失败次数和上次使用时间排序。"""
        ready = self.available()
        if not ready:
            raise Wait(10, "没有可用身份")
        if for_listing:
            ready = [identity for identity in ready if identity.listing_refresh_at is None]
            if not ready:
                raise crawl_identity_hold()
        identity = min(
            ready,
            key=lambda item: (
                item.id != preferred_id,
                item.id not in preferred_ids,
                item.consecutive_failures,
                item.last_used_at,
            ),
        )
        identity.busy = True
        return identity

    async def take_for_download(self) -> Identity:
        """等正在取链或采集的身份释放。没有有效会话时，交回调度中心稍后重试。"""
        try:
            async with asyncio.timeout(DOWNLOAD_IDENTITY_WAIT):
                while True:
                    self._available_changed.clear()
                    try:
                        return self.take()
                    except Wait:
                        now = time.time()
                        if not any(self._usable_session(i, now) for i in self.identities.values()):
                            raise
                    await self._available_changed.wait()
        except TimeoutError:
            raise Wait(10, "等待身份取链超时") from None

    def download_credentials(self, identity_id: str) -> DownloadCredentials | None:
        """读取可用会话，不占用资料目录。采集在进行时也可以复用同身份的缓存。"""
        identity = self.identities.get(identity_id)
        if identity is None or not self._usable_session(identity, time.time()):
            return None
        session = identity.session
        assert session is not None and session.token
        return session.user_agent, session.token

    def give_back(
        self,
        identity: Identity,
        result: Literal["neutral", "ok", "blocked", "listing_ok", "empty_response"] = "neutral",
    ) -> None:
        """归还身份，并根据下载或作品列表的结果更新对应的失败计数。"""
        now = time.time()
        identity.busy, identity.last_used_at = False, now
        self._available_changed.set()
        if result == "blocked":
            self._block(identity, now)
        elif identity.cooldown_until <= now:
            if result in ("ok", "listing_ok"):
                identity.fails = 0
                if result == "listing_ok":
                    identity.listing_fails = 0
                    identity.listing_refresh_at = None
            elif result == "empty_response":
                identity.listing_fails += 1
                if identity.listing_fails >= COUNTED_FAILS:
                    self._browsers.invalidate(self.profile(identity))
                    if self._listing_hold:
                        identity.listing_refresh_at = time.monotonic() + self._listing_hold
                        self._wake.set()
                    else:
                        identity.cooldown_until = now + COOLDOWN
                        identity.session = None
        self._save_use(identity, now)

    def record_download(
        self, identity_id: str, *, blocked: bool, succeeded: bool = False, token: str | None = None
    ) -> None:
        """回记下载结果，保留该身份当前的借用状态。"""
        self.note("download", not blocked)
        identity = self.identities.get(identity_id)
        if identity is None:
            return
        now = time.time()
        if blocked:
            self._block(identity, now, token)
        elif (
            succeeded
            and identity.cooldown_until <= now
            and identity.session is not None
            and (token is None or identity.session.token == token)
        ):
            # 并发下载中的旧会话成功不能清掉新会话的失败次数。
            identity.fails = 0
        else:
            return
        self._save_use(identity, now)

    def _block(self, identity: Identity, now: float, token: str | None = None) -> None:
        identity.fails += 1
        identity.cooldown_until = now + min(COOLDOWN * 2 ** (identity.fails - 1), MAX_COOLDOWN)
        if token is None or (identity.session is not None and identity.session.token == token):
            identity.session = None
        self._wake.set()
        self._available_changed.set()

    def _save_use(self, identity: Identity, now: float) -> None:
        if identity.fails >= RETIRE_FAILS and not identity.busy:
            return self.retire(identity, f"下载连续 {identity.fails} 次被拒")
        self._dirty[identity.id] = identity.record()
        if not identity.fresh(now, REFRESH_AFTER):
            self._wake.set()

    def refreshed(self, identity: Identity, session: Session) -> None:
        """用浏览器时顺带拿到了新的 Cookie。"""
        now = time.time()
        if session.token and identity.cooldown_until <= now:
            identity.session, identity.warmed_at = session, now
            identity.warm_retry_at = 0

    def refresh(self, identity: Identity) -> None:
        """账号池页面要求立即刷新：当作过期，后台循环下一个就预热它。"""
        self._browsers.invalidate(self.profile(identity))
        if identity.listing_refresh_at is None:
            identity.warmed_at = 0
        identity.warm_retry_at = 0
        self._wake.set()
        self._available_changed.set()

    # ---- 后台补充和刷新 ----

    def configure(self, target: int, warm_slots: int) -> None:
        if (target, warm_slots) != (self.target, self.warm_slots):
            self.target, self.warm_slots = target, warm_slots
            self._wake.set()

    def _under_pressure(self, now: float) -> bool:
        return self._crawl_policy and (
            now < self._warm_pressure_until
            or any(identity.listing_fails >= COUNTED_FAILS for identity in self.identities.values())
        )

    def _warm_delay(self, now: float) -> float:
        until = self._warm_wait_until
        if (
            self._warm_recovery
            and self._last_warm_started is not None
            and self._under_pressure(now)
        ):
            until = max(until, self._last_warm_started + self._warm_recovery)
        return max(0, until - now)

    async def run(self) -> None:
        jobs: set[asyncio.Task[None]] = set()

        def finished(job: asyncio.Task[None]) -> None:
            jobs.discard(job)
            self._wake.set()

        try:
            async with asyncio.TaskGroup() as group:
                while True:
                    self._wake.clear()
                    now = time.monotonic()
                    delay = self._warm_delay(now)
                    slots = self.warm_slots - len(jobs)
                    if self._warm_recovery and self._under_pressure(now):
                        slots = min(slots, 1 - len(jobs))
                    for _ in range(0 if delay else slots):
                        identity = self._next()
                        if identity is None:
                            break
                        identity.busy = True
                        self._warming[identity.id] = identity
                        job = group.create_task(self._attempt(identity))
                        jobs.add(job)
                        job.add_done_callback(finished)
                    with suppress(TimeoutError):
                        await asyncio.wait_for(self._wake.wait(), delay or CHECK_EVERY)
        finally:
            # 关闭时也释放已预留但还未来得及启动的身份。
            for identity in self._warming.values():
                identity.busy = False
            self._warming.clear()

    async def _attempt(self, identity: Identity) -> None:
        retry_delay = CHECK_EVERY
        retry_profile = True
        profile = self.profile(identity)
        created_here = False
        try:
            if self._keep_profile_dirs and identity.id not in self.identities:
                with suppress(FileExistsError):
                    profile.mkdir(parents=True)
                    created_here = True
            if await self._warm(identity):
                return
        except Wait as wait:
            if wait.code == WARM_LOCAL_WAIT:
                return
            log.info("预热等线路：%s", wait.reason)
            if self._crawl_policy and wait.code == WARM_EMPTY_WAIT:
                until = time.monotonic() + max(1, wait.seconds)
                self._warm_wait_until = max(self._warm_wait_until, until)
                self._warm_pressure_until = max(
                    self._warm_pressure_until, until + self._warm_recovery
                )
                self._wake.set()
                return
            retry_delay = max(retry_delay, wait.seconds)
            retry_profile = False
        except Exception:
            log.exception("身份 %s 预热异常", identity.id)
        finally:
            identity.busy = False
            self._warming.pop(identity.id)
            self._available_changed.set()
            if identity.id in self.identities and identity.fails >= RETIRE_FAILS:
                self._save_use(identity, time.time())
            elif identity.id not in self.identities and (
                not self._keep_profile_dirs or (created_here and identity.retired_at is None)
            ):
                shutil.rmtree(profile, ignore_errors=True)
        if retry_profile:
            identity.warm_retry_at = time.time() + WARM_RETRY_AFTER
        await asyncio.sleep(retry_delay)

    def _next(self) -> Identity | None:
        """先恢复缺少的可用身份，再提前刷新还有效的会话。"""
        if not self.target:
            return None
        now, monotonic_now = time.time(), time.monotonic()

        def ready(identity: Identity) -> bool:
            return identity.listing_refresh_at is None and self._usable_session(identity, now)

        available = sum(
            identity.fails < COUNTED_FAILS and ready(identity)
            for identity in self.identities.values()
        )
        candidates = [
            identity
            for identity in self.identities.values()
            if not identity.busy
            and identity.cooldown_until <= now
            and identity.warm_retry_at <= now
            and (
                identity.listing_refresh_at is None or identity.listing_refresh_at <= monotonic_now
            )
        ]
        if available >= self.target:
            return min(
                (
                    identity
                    for identity in candidates
                    if identity.listing_refresh_at is not None
                    or not identity.fresh(now, REFRESH_AFTER)
                ),
                key=lambda identity: (identity.listing_refresh_at is None, identity.warmed_at),
                default=None,
            )
        # 正在刷新但仍有有效会话的身份已计在 available 里。
        pending = sum(
            identity.fails < COUNTED_FAILS and not ready(identity)
            for identity in self._warming.values()
        )
        if available + pending >= self.target:
            return None
        stale = min(
            (identity for identity in candidates if not ready(identity)),
            key=lambda identity: (
                identity.listing_refresh_at is None,
                identity.fails,
                identity.warmed_at,
            ),
            default=None,
        )
        if stale is not None and stale.fails < COUNTED_FAILS:
            return stale
        if self.identities and self._under_pressure(monotonic_now):
            return stale or min(
                (identity for identity in candidates if not identity.fresh(now, REFRESH_AFTER)),
                key=lambda identity: identity.warmed_at,
                default=None,
            )
        pending_new = sum(key not in self.identities for key in self._warming)
        if len(self.identities) + pending_new < self.target * MAX_FACTOR:
            return Identity(uuid4().hex[:12])
        return stale

    async def _warm(self, identity: Identity) -> bool:
        """预热：打开首页等页面数据出现，拿到 tt_chain_token 才算成功。新建的成功后才加入身份池。"""
        options = {}
        session_now = time.time()
        if self._crawl_policy and not any(
            item.listing_refresh_at is None and self._usable_session(item, session_now)
            for item in self.identities.values()
        ):
            options["warm_bootstrap"] = True
        lease = await self._scheduler.lease(CRAWL, avoid=self._avoid, **options)
        session, detail = None, "预热没拿到 tt_chain_token"
        started = skipped = False
        try:
            self._check_warm_start()
            async with self._browsers.open(
                self.profile(identity), lease.route, warming=True
            ) as page:
                # 等 Chrome 槽位期间可能进入压力期。首页发出前再次确认，按实际发出计时。
                now = self._check_warm_start()
                if self._crawl_policy:
                    self._record_warm_start(now)
                self._active_warms += 1
                started = True
                await page.goto(tiktok.HOME, wait_until="domcontentloaded", timeout=WARM_TIMEOUT)
                await page.wait_for_selector(
                    f"#{tiktok.DATA_ID}", state="attached", timeout=WARM_TIMEOUT
                )
                session = await session_of(page)
        except Wait as wait:
            skipped = wait.code == WARM_LOCAL_WAIT
            raise
        except (BrowserError, Retry) as error:
            detail = f"预热失败：{error}"
        finally:
            if started:
                self._active_warms -= 1
            ok = bool(session and session.token)
            if not skipped:
                self.note("warm", ok)
            await self._scheduler.release(
                lease, "normal" if ok or skipped else "suspect", None if ok or skipped else detail
            )
        if not ok:
            self._avoid = [lease.line]
            log.info("身份 %s %s", identity.id, detail)
            return False
        self._avoid = []
        log.info("身份 %s 预热好了", identity.id)
        self.browser = identity.browser = session.browser
        self.refreshed(identity, session)
        if not self._crawl_policy or identity.session is session:
            identity.listing_fails = 0
            identity.listing_refresh_at = None
        self._dirty[identity.id] = identity.record()
        self.identities[identity.id] = identity
        for other in list(self.identities.values()):
            if other.browser != self.browser and not other.busy:
                self.retire(other, f"Chrome 从 {other.browser} 换成了 {self.browser}")
        return True

    def _check_warm_start(self) -> float:
        now = time.monotonic()
        if self._warm_delay(now) or (
            self._warm_recovery and self._active_warms and self._under_pressure(now)
        ):
            raise Wait(1, "本机预热等待，首页请求尚未发出", code=WARM_LOCAL_WAIT)
        return now

    def _record_warm_start(self, now: float) -> None:
        self._last_warm_started = now
        if self._under_pressure(now):
            self._warm_pressure_until = max(self._warm_pressure_until, now + self._warm_recovery)

    def retire(self, identity: Identity, reason: str) -> None:
        """退休身份，后台会补一个新的。"""
        identity.retired_at, identity.retire_reason = time.time(), reason
        self.identities.pop(identity.id, None)
        if not self._keep_profile_dirs:
            shutil.rmtree(self.profile(identity), ignore_errors=True)
        self._dirty[identity.id] = identity.record()
        self._retired.append(identity)
        log.info("身份 %s 退休：%s", identity.id, reason)
        self._wake.set()
        self._available_changed.set()
