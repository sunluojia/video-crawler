import asyncio
import time
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from patchright._impl._errors import TargetClosedError
from patchright.async_api import Error as BrowserError
from pydantic import ValidationError

from video_crawler import browser, crawl, identities, tiktok
from video_crawler.browser import Browsers, Session
from video_crawler.errors import Wait
from video_crawler.identities import Identity, IdentityPool
from video_crawler.scheduler import ClaimedTask, Lease
from video_crawler.settings import Settings

DIRECT = {"kind": "direct"}
PROXY = {
    "kind": "proxy",
    "host": "private-proxy.test",
    "port": 12345,
    "protocol": "http",
    "username": "proxy-user",
    "password": "proxy-password",
}


class Events:
    def __init__(self):
        self.listeners = {}

    def on(self, event, callback):
        self.listeners.setdefault(event, []).append(callback)

    def remove_listener(self, event, callback):
        self.listeners[event].remove(callback)
        if not self.listeners[event]:
            del self.listeners[event]

    def emit(self, event):
        for callback in self.listeners.get(event, ()):
            callback(self)


class Tab(Events):
    def __init__(self, context):
        super().__init__()
        self.context = context
        self.url = "about:blank"
        self.closed = False
        self.routes = {}
        self.goto = AsyncMock()
        self.wait_for_selector = AsyncMock()

    async def route(self, glob, callback):
        self.routes[glob] = callback

    async def unroute_all(self, *, behavior):
        assert behavior == "ignoreErrors"
        self.routes.clear()

    def is_closed(self):
        return self.closed

    async def close(self):
        self.closed = True
        self.context.pages.remove(self)


class Context(Events):
    def __init__(self):
        super().__init__()
        self.pages = [Tab(self)]
        self.close_calls = 0
        self.closing = asyncio.Event()
        self.closed = asyncio.Event()
        self.may_close = asyncio.Event()
        self.may_close.set()

    async def new_page(self):
        page = Tab(self)
        self.pages.append(page)
        return page

    def is_closed(self):
        return self.closed.is_set()

    async def close(self):
        self.close_calls += 1
        self.closing.set()
        await self.may_close.wait()
        for page in list(self.pages):
            await page.close()
        self.closed.set()
        self.emit("close")


@pytest.fixture
async def make_browser(monkeypatch):
    managers = []

    async def make(*, limit=2, **options):
        contexts, forwarders = [], []

        @asynccontextmanager
        async def proxy(route):
            closed = asyncio.Event()
            forwarders.append(closed)
            try:
                yield None if route == DIRECT else {"server": "http://127.0.0.1:1234"}
            finally:
                closed.set()

        async def launch(*args, **kwargs):
            context = Context()
            contexts.append(context)
            return context

        async def stop():
            assert all(context.closed.is_set() for context in contexts)
            assert all(forwarder.is_set() for forwarder in forwarders)

        monkeypatch.setattr(browser, "browser_proxy", proxy)
        manager = Browsers("chrome", **options)
        runtime = SimpleNamespace(
            contexts=contexts,
            forwarders=forwarders,
            chromium=SimpleNamespace(launch_persistent_context=AsyncMock(side_effect=launch)),
            stop=AsyncMock(side_effect=stop),
        )
        manager._playwright = runtime
        await manager.resize(limit)
        managers.append(manager)
        return manager, runtime

    yield make
    for manager in managers:
        await manager.stop()


@pytest.fixture
def existing_profile(tmp_path):
    profile = tmp_path / "abcdef123456"
    profile.mkdir()
    (profile / "profile-marker").write_text("existing Chrome profile", encoding="utf-8")
    yield profile
    assert (profile / "profile-marker").read_text(encoding="utf-8") == "existing Chrome profile"


@pytest.mark.parametrize(
    ("options", "blocked"),
    [
        ({}, {"image", "media", "font"}),
        ({"block_nonessential_resources": False}, set()),
        ({"blocked_resource_types": ("media",)}, {"media"}),
    ],
    ids=["default", "off", "custom"],
)
async def test_crawl_intercepts_only_the_configured_nonessential_resources(
    make_browser, existing_profile, caplog, options, blocked
):
    manager, _ = await make_browser(**options)
    caplog.set_level("INFO", logger=browser.__name__)
    async with manager.open_crawl(existing_profile, PROXY, line="private-line") as page:
        if blocked:
            handler = page.routes["**/*"]
            for resource_type in (
                "image",
                "media",
                "font",
                "document",
                "script",
                "xhr",
                "fetch",
                "stylesheet",
                "websocket",
                "other",
                "manifest",
                "texttrack",
                "eventsource",
            ):
                request = SimpleNamespace(
                    resource_type=resource_type, url="https://example.test/?secret=tracking-value"
                )
                route = SimpleNamespace(request=request, abort=AsyncMock(), continue_=AsyncMock())
                await handler(route)
                if resource_type in blocked:
                    route.abort.assert_awaited_once_with()
                    route.continue_.assert_not_awaited()
                else:
                    route.continue_.assert_awaited_once_with()
                    route.abort.assert_not_awaited()
        else:
            assert not page.routes
    assert not page.routes
    assert caplog.messages == [f"crawl_resources enabled={bool(blocked)} blocked={len(blocked)}"]


async def test_same_profile_and_line_reuse_chrome_but_close_each_creator_tab(
    make_browser, existing_profile
):
    manager, runtime = await make_browser(limit=1)
    async with manager.open_crawl(existing_profile, PROXY, line="line") as first:
        context = first.context
        popup = await context.new_page()
        first.url = "https://example.test/first"
        popup.url = "https://example.test/popup"
        assert "crash" in first.listeners and first.routes
    assert first.closed and popup.closed and not first.listeners and not first.routes
    assert [page.url for page in context.pages] == ["about:blank"]
    assert not runtime.forwarders[0].is_set()

    reordered = dict(reversed(list(PROXY.items())))
    async with manager.open_crawl(existing_profile, reordered, line="line") as second:
        assert second is not first and second.context is context
        assert second.url == "about:blank"
    assert second.closed and not second.listeners and not second.routes
    runtime.chromium.launch_persistent_context.assert_awaited_once()
    assert context.close_calls == 0
    assert len(runtime.forwarders) == 1


@pytest.mark.parametrize(
    ("new_line", "route_overrides"),
    [("other", {}), ("line", {"port": 321}), ("line", {"password": "rotated"})],
    ids=["line", "endpoint", "credentials"],
)
async def test_line_or_proxy_change_closes_chrome_and_its_forwarder_before_launching(
    make_browser, existing_profile, new_line, route_overrides
):
    manager, runtime = await make_browser(limit=1)
    async with manager.open_crawl(existing_profile, PROXY, line="line") as page:
        previous = page.context
    async with manager.open_crawl(existing_profile, PROXY | route_overrides, line=new_line) as page:
        assert page.context is not previous
        assert previous.closed.is_set() and runtime.forwarders[0].is_set()
    assert len(runtime.contexts) == 2


async def test_same_identity_has_only_one_borrower_even_with_different_proxies(
    make_browser, existing_profile
):
    manager, runtime = await make_browser(limit=2)
    second_requested = asyncio.Event()
    second_entered = asyncio.Event()

    async def second_task():
        second_requested.set()
        alias = existing_profile / ".." / existing_profile.name
        async with manager.open_crawl(alias, PROXY, line="other") as page:
            second_entered.set()
            assert page.context is not first.context
            assert first.context.closed.is_set()

    async with manager.open_crawl(existing_profile, DIRECT, line="direct") as first:
        waiting = asyncio.create_task(second_task())
        await asyncio.wait_for(second_requested.wait(), 1)
        assert not second_entered.is_set()
        runtime.chromium.launch_persistent_context.assert_awaited_once()
    await asyncio.wait_for(waiting, 1)
    assert second_entered.is_set()


@pytest.mark.parametrize("warming", [False, True], ids=["download-address", "warm"])
async def test_cold_navigation_waits_for_the_same_profile_and_never_filters_resources(
    make_browser, existing_profile, monkeypatch, warming
):
    manager, runtime = await make_browser(warm_headless=True)
    monkeypatch.setattr(manager, "_headless_agent", AsyncMock(return_value="Chrome/146"))
    requested = asyncio.Event()

    async def open_cold():
        requested.set()
        async with manager.open(existing_profile, PROXY, warming=warming) as page:
            assert creator.context.closed.is_set() and runtime.forwarders[0].is_set()
            assert not page.routes and not page.listeners
            assert page.context.pages == [page]
            return page.context

    async with manager.open_crawl(existing_profile, DIRECT, line="direct") as creator:
        waiting = asyncio.create_task(open_cold())
        await asyncio.wait_for(requested.wait(), 1)
        runtime.chromium.launch_persistent_context.assert_awaited_once()

    context = await asyncio.wait_for(waiting, 1)
    assert context.closed.is_set() and runtime.forwarders[1].is_set()
    launch_options = runtime.chromium.launch_persistent_context.await_args.kwargs
    assert launch_options["headless"] is warming
    assert launch_options.get("user_agent") == ("Chrome/146" if warming else None)


@pytest.mark.parametrize("failure", ["crash-event", "tab-closed", "target-closed"])
async def test_unhealthy_page_evicts_the_context_and_the_next_crawl_rebuilds_it(
    make_browser, existing_profile, failure
):
    manager, runtime = await make_browser()
    expected = pytest.raises(TargetClosedError) if failure == "target-closed" else nullcontext()
    with expected:
        async with manager.open_crawl(existing_profile, DIRECT, line="line") as page:
            if failure == "crash-event":
                page.emit("crash")
            elif failure == "tab-closed":
                await page.close()
            else:
                raise TargetClosedError()
    assert page.context.closed.is_set() and not page.listeners
    async with manager.open_crawl(existing_profile, DIRECT, line="line") as rebuilt:
        assert rebuilt.context is not page.context
    assert len(runtime.contexts) == 2


async def test_an_idle_browser_disconnect_evicts_its_forwarder(make_browser, existing_profile):
    manager, runtime = await make_browser()
    async with manager.open_crawl(existing_profile, PROXY, line="line") as page:
        original = page.context
    original.emit("close")
    await asyncio.wait_for(runtime.forwarders[0].wait(), 1)
    assert original.closed.is_set()
    async with manager.open_crawl(existing_profile, PROXY, line="line") as page:
        assert page.context is not original


async def test_context_is_closed_on_its_last_allowed_use(make_browser, existing_profile):
    manager, runtime = await make_browser(browser_context_max_uses=2)
    async with manager.open_crawl(existing_profile, DIRECT, line="line") as first:
        original = first.context
    async with manager.open_crawl(existing_profile, DIRECT, line="line") as second:
        assert second.context is original
    assert original.closed.is_set()
    async with manager.open_crawl(existing_profile, DIRECT, line="line") as third:
        assert third.context is not original
    assert len(runtime.contexts) == 2


async def test_idle_expiration_closes_before_rebuilding_the_same_profile(
    make_browser, existing_profile
):
    manager, runtime = await make_browser(browser_context_idle_ttl_seconds=0.02)
    async with manager.open_crawl(existing_profile, DIRECT, line="line") as first:
        original = first.context
    original.may_close.clear()
    try:
        await asyncio.wait_for(original.closing.wait(), 1)
        requested = asyncio.Event()

        async def replacement():
            requested.set()
            async with manager.open_crawl(existing_profile, DIRECT, line="line") as page:
                assert original.closed.is_set() and page.context is not original

        job = asyncio.create_task(replacement())
        await asyncio.wait_for(requested.wait(), 1)
        runtime.chromium.launch_persistent_context.assert_awaited_once()
    finally:
        original.may_close.set()
    await asyncio.wait_for(job, 1)
    assert original.close_calls == 1


async def test_idle_contexts_make_room_for_other_profiles_and_limit_changes(
    make_browser, existing_profile, tmp_path
):
    manager, _ = await make_browser(limit=2)
    async with manager.open_crawl(existing_profile, DIRECT, line="line") as first:
        pass
    async with manager.open_crawl(tmp_path / "second", DIRECT, line="line") as second:
        pass
    async with asyncio.timeout(1), manager.open(tmp_path / "download", DIRECT) as download:
        assert first.context.closed.is_set()
        assert not second.context.closed.is_set()
        await manager.resize(0)
        await second.context.closed.wait()
    assert download.context.closed.is_set()


async def test_shutdown_closes_idle_and_borrowed_contexts_before_stopping_the_driver(
    make_browser, existing_profile, tmp_path
):
    manager, runtime = await make_browser()
    async with manager.open_crawl(tmp_path / "idle", PROXY, line="line"):
        pass
    async with manager.open_crawl(existing_profile, DIRECT, line="direct"):
        requested = asyncio.Event()

        async def wait_for_profile():
            requested.set()
            async with manager.open_crawl(existing_profile, DIRECT, line="direct"):
                pytest.fail("borrow during shutdown")

        waiting = asyncio.create_task(wait_for_profile())
        await asyncio.wait_for(requested.wait(), 1)
        await asyncio.wait_for(manager.stop(), 1)
        with pytest.raises(BrowserError, match="正在停止"):
            await asyncio.wait_for(waiting, 1)
    assert all(context.close_calls == 1 and not context.listeners for context in runtime.contexts)
    runtime.stop.assert_awaited_once()


@pytest.mark.parametrize("block_resources", [True, False])
async def test_reuse_off_starts_and_closes_a_context_for_every_task(
    make_browser, existing_profile, block_resources
):
    manager, runtime = await make_browser(
        reuse_browser_context=False, block_nonessential_resources=block_resources
    )
    async with manager.open_crawl(existing_profile, PROXY, line="line") as first:
        assert first.context.pages == [first] and bool(first.routes) is block_resources
    assert first.closed and first.context.closed.is_set() and runtime.forwarders[0].is_set()
    async with manager.open_crawl(existing_profile, PROXY, line="line") as second:
        assert second.context.pages == [second] and second.context is not first.context
    assert second.closed and second.context.closed.is_set() and runtime.forwarders[1].is_set()
    assert len(runtime.contexts) == 2


async def test_cancelled_launch_closes_chrome_before_the_profile_can_be_borrowed_again(
    make_browser, existing_profile
):
    manager, runtime = await make_browser()
    started, may_finish, second_requested = asyncio.Event(), asyncio.Event(), asyncio.Event()
    launch = runtime.chromium.launch_persistent_context
    original_launch = launch.side_effect

    async def deferred_launch(*args, **kwargs):
        started.set()
        await may_finish.wait()
        return await original_launch(*args, **kwargs)

    async def cancelled_crawl():
        async with manager.open_crawl(existing_profile, DIRECT, line="line"):
            pytest.fail("expected cancellation before the tab was created")

    async def next_crawl():
        second_requested.set()
        async with manager.open_crawl(existing_profile, DIRECT, line="line") as page:
            assert runtime.contexts[0].closed.is_set() and runtime.forwarders[0].is_set()
            assert page.context is runtime.contexts[1]

    launch.side_effect = deferred_launch
    first = asyncio.create_task(cancelled_crawl())
    try:
        await asyncio.wait_for(started.wait(), 1)
        first.cancel()
        second = asyncio.create_task(next_crawl())
        await asyncio.wait_for(second_requested.wait(), 1)
        launch.assert_awaited_once()
    finally:
        may_finish.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(first, 1)
    await asyncio.wait_for(second, 1)


async def test_empty_listing_recovery_rebuilds_chrome_and_keeps_the_download_session(
    make_browser, existing_profile, monkeypatch
):
    manager, runtime = await make_browser(limit=1)
    settings = Settings(
        _env_file=None,
        service_token="test",
        profile_root=existing_profile.parent,
        crawl_policy_enabled=True,
        keep_profile_dirs=True,
    )
    lease = Lease("lease", "line", PROXY)
    scheduler = SimpleNamespace(lease=AsyncMock(return_value=lease), release=AsyncMock())
    pool = IdentityPool(settings, Mock(), scheduler, manager)
    pool.target, pool.browser = 1, "146"
    original_session = Session(
        {"tt_chain_token": "original", "msToken": "original-ms"}, "ua", "146"
    )
    identity = Identity(
        existing_profile.name, browser="146", warmed_at=time.time(), session=original_session
    )
    pool.identities[identity.id] = identity
    now = 100.0
    monkeypatch.setattr(identities, "time", SimpleNamespace(time=time.time, monotonic=lambda: now))
    creator = ClaimedTask(
        "task",
        1,
        "crawl_creator",
        "attempt",
        {
            "creator": {"handle": "test"},
            "max_works": 5,
        },
    )

    async def empty_listing(page, body, line, *args, **kwargs):
        assert page.routes
        raise crawl.ListingFailure("empty", line, "empty_response")

    monkeypatch.setattr(crawl, "_read", empty_listing)
    crawler = crawl.Crawler(pool, scheduler, manager)
    for _ in range(identities.COUNTED_FAILS):
        with pytest.raises(crawl.ListingFailure):
            await crawler(creator)
        assert scheduler.release.await_args.kwargs["listing_result"] == "empty_response"

    [first] = runtime.contexts
    await asyncio.wait_for(runtime.forwarders[0].wait(), 1)
    assert first.closed.is_set()
    assert identity.session is original_session
    assert pool.download_credentials(identity.id) == ("ua", "original")
    with pytest.raises(Wait) as held:
        pool.take(for_listing=True)
    assert held.value.code == "crawl_identity_hold"
    assert pool._next() is None

    now += settings.crawl_empty_session_hold_seconds
    assert pool._next() is identity
    renewed = Session({"tt_chain_token": "renewed", "msToken": "renewed-ms"}, "ua", "146")
    session_of = AsyncMock(return_value=renewed)
    monkeypatch.setattr(identities, "session_of", session_of)
    assert await pool._warm(identity)
    [warm_page] = session_of.await_args.args
    warm_page.goto.assert_awaited_once_with(
        tiktok.HOME, wait_until="domcontentloaded", timeout=identities.WARM_TIMEOUT
    )
    assert not warm_page.routes and warm_page.context.closed.is_set()
    assert identity.session is renewed and identity.listing_fails == 0
    assert identity.listing_refresh_at is None and pool.available_for_crawl() == [identity]

    async with manager.open_crawl(existing_profile, PROXY, line="line") as rebuilt:
        assert rebuilt.context is runtime.contexts[2]


def test_browser_settings_read_env_and_reject_blocking_core_resources(monkeypatch):
    monkeypatch.setenv("VC_BLOCK_NONESSENTIAL_RESOURCES", "false")
    monkeypatch.setenv("VC_BLOCKED_RESOURCE_TYPES", '["font"]')
    monkeypatch.setenv("VC_REUSE_BROWSER_CONTEXT", "false")
    monkeypatch.setenv("VC_BROWSER_CONTEXT_MAX_USES", "50")
    monkeypatch.setenv("VC_BROWSER_CONTEXT_IDLE_TTL_SECONDS", "150")
    options = Settings(_env_file=None, service_token="test")
    assert options.block_nonessential_resources is False
    assert options.blocked_resource_types == ("font",)
    assert options.reuse_browser_context is False
    assert options.browser_context_max_uses == 50
    assert options.browser_context_idle_ttl_seconds == 150

    monkeypatch.setenv("VC_BLOCKED_RESOURCE_TYPES", '["media", "script"]')
    with pytest.raises(ValidationError, match="blocked_resource_types"):
        Settings(_env_file=None, service_token="test")


def test_warm_recovery_interval_compatibility_alias(monkeypatch):
    monkeypatch.delenv("VC_CRAWL_WARM_RECOVERY_MIN_SECONDS", raising=False)
    monkeypatch.setenv("VC_WARM_MIN_INTERVAL_SECONDS", "147")
    assert Settings(_env_file=None, service_token="test").crawl_warm_recovery_min_seconds == 147
    monkeypatch.setenv("VC_CRAWL_WARM_RECOVERY_MIN_SECONDS", "175")
    assert Settings(_env_file=None, service_token="test").crawl_warm_recovery_min_seconds == 175
