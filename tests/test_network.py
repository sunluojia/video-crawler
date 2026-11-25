"""线路怎么连、视频怎么算下完整、代理测速怎么算下完。"""

import asyncio
import hashlib
from base64 import b64encode
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from video_crawler import download
from video_crawler.browser import Browsers
from video_crawler.download import Broken, Skip, Stop
from video_crawler.errors import Retry
from video_crawler.proxycheck import Incomplete, proxy_address
from video_crawler.proxycheck import download as speed_download
from video_crawler.routes import browser_proxy, proxy_url

MP4 = b"\x00\x00\x00\x18ftypmp42" + b"x" * 1000


async def test_proxy_urls_for_each_kind_of_route():
    assert await proxy_url({"kind": "direct"}) is None
    route = {
        "kind": "proxy",
        "protocol": "socks5",
        "host": "1.2.3.4",
        "port": 1080,
        "username": "alice",
        "password": "p@ss",
    }
    assert await proxy_url(route) == "socks5h://alice:p%40ss@1.2.3.4:1080"
    with pytest.raises(Retry, match="SSH"):
        await proxy_url(route | {"protocol": "ssh"})


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while data := await reader.read(65536):
        writer.write(data)
        await writer.drain()
    writer.close()


async def test_the_browser_goes_through_a_local_forwarder_that_adds_the_password():
    async def echo(reader, writer):
        writer.write(await reader.read(5))
        await writer.drain()
        writer.close()

    async def upstream(reader, writer):  # 要口令的 HTTP 代理
        head = await reader.readuntil(b"\r\n\r\n")
        if b"Proxy-Authorization: Basic " + b64encode(b"alice:p@ss") not in head:
            writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
            writer.close()
            return
        host, port = head.split(b" ")[1].decode().rsplit(":", 1)
        remote_reader, remote_writer = await asyncio.open_connection(host, int(port))
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await asyncio.gather(_pipe(reader, remote_writer), _pipe(remote_reader, writer))

    target = await asyncio.start_server(echo, "127.0.0.1", 0)
    proxy = await asyncio.start_server(upstream, "127.0.0.1", 0)
    route = {
        "kind": "proxy",
        "protocol": "http",
        "host": "127.0.0.1",
        "port": proxy.sockets[0].getsockname()[1],
        "username": "alice",
        "password": "p@ss",
    }
    async with browser_proxy(route) as settings:
        port = int(settings["server"].rsplit(":", 1)[1])
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            f"CONNECT 127.0.0.1:{target.sockets[0].getsockname()[1]} HTTP/1.1\r\n\r\n".encode()
        )
        assert (await reader.readuntil(b"\r\n\r\n")).startswith(b"HTTP/1.1 200")
        writer.write(b"hello")
        assert await reader.read(5) == b"hello"
        writer.close()
    target.close()
    proxy.close()


async def test_closing_the_browser_also_closes_connections_still_waiting_for_the_proxy():
    asked, closed = asyncio.Event(), asyncio.Event()

    async def silent(reader, writer):  # 收到 CONNECT 后一直不回
        await reader.readuntil(b"\r\n\r\n")
        asked.set()
        await reader.read()  # 等转发口关掉这条连接
        closed.set()

    proxy = await asyncio.start_server(silent, "127.0.0.1", 0)
    route = {
        "kind": "proxy",
        "protocol": "http",
        "host": "127.0.0.1",
        "port": proxy.sockets[0].getsockname()[1],
        "username": "alice",
        "password": "secret",
    }
    async with browser_proxy(route) as settings:
        port = int(settings["server"].rsplit(":", 1)[1])
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"CONNECT example.test:443 HTTP/1.1\r\n\r\n")
        await asyncio.wait_for(asked.wait(), 1)
        writer.close()  # 浏览器那头先断了
    await asyncio.wait_for(closed.wait(), 1)
    proxy.close()


async def test_a_proxy_without_password_goes_to_the_browser_as_is():
    route = {"kind": "proxy", "protocol": "socks5", "host": "127.0.0.1", "port": 11479}
    async with browser_proxy(route) as settings:
        assert settings == {"server": "socks5://127.0.0.1:11479"}


@pytest.mark.parametrize(
    ("route", "expected_args", "expected_proxy"),
    [
        ({"kind": "direct"}, ["--no-proxy-server"], None),
        (
            {"kind": "proxy", "protocol": "socks5", "host": "127.0.0.1", "port": 11479},
            None,
            {"server": "socks5://127.0.0.1:11479"},
        ),
    ],
)
async def test_browser_uses_the_granted_route(tmp_path, route, expected_args, expected_proxy):
    page = object()
    context = SimpleNamespace(pages=[page], close=AsyncMock())
    launch = AsyncMock(return_value=context)
    browsers = Browsers("chrome")
    browsers._playwright = SimpleNamespace(
        chromium=SimpleNamespace(launch_persistent_context=launch)
    )
    await browsers.resize(1)

    async with browsers.open(tmp_path, route) as opened:
        assert opened is page

    assert launch.await_args.kwargs.get("args") == expected_args
    assert launch.await_args.kwargs["proxy"] == expected_proxy


def streamed(status: int, body: bytes = b"", **headers: str) -> httpx.Response:
    """像真实网络那样边收边读的响应，默认声明正文长度。"""

    async def chunks():
        yield body

    headers = {"Content-Length": str(len(body))} | {
        k.replace("_", "-"): v for k, v in headers.items()
    }
    return httpx.Response(status, content=chunks(), headers=headers)


def media_client(response: httpx.Response) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: response),
        event_hooks={"request": [download.trusted]},
    )


async def test_a_complete_video_is_saved_with_its_sha256(tmp_path):
    response = streamed(200, MP4, Content_Type="application/octet-stream")
    path = tmp_path / "v.mp4"
    sha256 = await download.transfer(media_client(response), "https://v16.tiktokcdn.com/a", path)
    assert sha256 == hashlib.sha256(MP4).hexdigest()
    assert path.read_bytes() == MP4
    assert not path.with_suffix(".part").exists()


RANGE_VIDEO = MP4 + bytes(range(251)) * 4323
MEDIA_URL = "https://v16.tiktokcdn.com/a"


def requested_span(request: httpx.Request) -> tuple[int, int]:
    start, end = request.headers["Range"].removeprefix("bytes=").split("-")
    return int(start), int(end)


@pytest.mark.parametrize("parts", [3, 4])
async def test_range_transfers_run_concurrently_and_assemble_verified_bytes(
    tmp_path, parts, monkeypatch
):
    spans = []
    all_requested = asyncio.Event()
    total = len(RANGE_VIDEO)
    progress = download.DownloadProgress("task", "video")
    now = 0.0
    monkeypatch.setattr(download, "time", SimpleNamespace(monotonic=lambda: now))

    async def handle(request):
        if "Range" not in request.headers:

            async def unread_body():
                pytest.fail("The initial GET should only read headers before splitting")
                yield b""

            return httpx.Response(
                200,
                content=unread_body(),
                headers={
                    "Content-Length": str(total),
                    "Accept-Ranges": "Bytes",
                    "ETag": '"v1"',
                },
            )

        assert request.headers["If-Range"] == '"v1"'
        start, end = requested_span(request)
        spans.append((start, end))
        if len(spans) == parts:
            all_requested.set()
        await all_requested.wait()
        assert progress.total_bytes == total

        async def chunks():
            nonlocal now
            body = RANGE_VIDEO[start : end + 1]
            if progress.received_bytes + len(body) == total:
                # One shard alone falls below 20 KiB/s; the combined transfer stays above it.
                now = 45.0
            yield body

        return httpx.Response(
            206, content=chunks(), headers={"Content-Range": f"bytes {start}-{end}/{total}"}
        )

    path = tmp_path / "video.mp4"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        digest = await asyncio.wait_for(
            download.transfer(client, MEDIA_URL, path, progress, range_parts=parts), 2
        )

    assert sorted(spans) == [
        (i * total // parts, (i + 1) * total // parts - 1) for i in range(parts)
    ]
    assert progress.received_bytes == total
    assert digest == hashlib.sha256(RANGE_VIDEO).hexdigest()
    assert path.read_bytes() == RANGE_VIDEO
    assert not path.with_suffix(".part").exists()


async def test_a_full_stream_403_after_range_rejection_still_reports_a_block(tmp_path):
    full_requests = 0

    async def handle(request):
        nonlocal full_requests
        if "Range" not in request.headers:
            full_requests += 1
            if full_requests == 1:
                return streamed(200, MP4, Accept_Ranges="bytes")
        return streamed(403)

    path = tmp_path / "video.mp4"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(Stop, match="403") as error:
            await download.transfer(client, MEDIA_URL, path, range_min_bytes=1)

    assert error.value.blocked and full_requests == 2
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "problem",
    [
        "200",
        "403",
        "429",
        "wrong-range",
        "short",
        "too-long",
        "changed-etag",
        "encoded",
        "disconnect",
    ],
)
async def test_range_failure_retries_with_a_clean_single_stream(tmp_path, problem):
    progress = download.DownloadProgress("task", "video")
    full_requests = 0
    cancelled = asyncio.Event()
    another_started = asyncio.Event()
    ranges_started = 0
    total = len(MP4)

    async def handle(request):
        nonlocal full_requests, ranges_started
        if "Range" not in request.headers:
            full_requests += 1
            if full_requests == 2:
                assert cancelled.is_set()
                return streamed(200, MP4, Content_Type="video/mp4")
            return streamed(200, MP4, Accept_Ranges="bytes", ETag='"v1"')

        ranges_started += 1
        start, end = requested_span(request)
        body = MP4[start : end + 1]
        headers = {
            "Content-Range": f"bytes {start}-{end}/{total}",
            "Content-Length": str(len(body)),
        }
        if start != 0:

            async def unfinished():
                try:
                    yield body[:1]
                    another_started.set()
                    await asyncio.Future()
                finally:
                    cancelled.set()

            return httpx.Response(206, content=unfinished(), headers=headers)

        await another_started.wait()
        if problem.isdigit():
            return streamed(int(problem), body)
        if problem == "wrong-range":
            headers["Content-Range"] = f"bytes {start + 1}-{end + 1}/{total}"
        if problem == "changed-etag":
            headers["ETag"] = '"v2"'
        if problem == "encoded":
            headers["Content-Encoding"] = "gzip"

        async def faulty():
            if problem == "short":
                yield body[:-1]
            elif problem == "too-long":
                yield body + b"!"
            else:
                yield body
            if problem == "disconnect":
                raise httpx.ReadError("remote closed")

        return httpx.Response(206, content=faulty(), headers=headers)

    path = tmp_path / "video.mp4"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        digest = await asyncio.wait_for(
            download.transfer(client, MEDIA_URL, path, progress, range_parts=2, range_min_bytes=1),
            2,
        )

    assert (full_requests, ranges_started) == (2, 2)
    assert (progress.received_bytes, progress.total_bytes) == (total, total)
    assert digest == hashlib.sha256(MP4).hexdigest()
    assert path.read_bytes() == MP4
    assert not path.with_suffix(".part").exists()


@pytest.mark.parametrize(
    ("accept_ranges", "options"),
    [
        ("none", {"range_min_bytes": 1}),
        ("bytes", {"range_min_bytes": len(MP4) + 1}),
        ("bytes", {"range_min_bytes": 1, "range_parts": 1}),
    ],
    ids=["unsupported", "below-threshold", "disabled"],
)
async def test_single_stream_does_not_probe_when_range_is_ineligible(
    tmp_path, accept_ranges, options
):
    requests = []

    async def handle(request):
        requests.append(request)
        return streamed(200, MP4, Accept_Ranges=accept_ranges)

    path = tmp_path / "video.mp4"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert (
            await download.transfer(client, MEDIA_URL, path, **options)
            == hashlib.sha256(MP4).hexdigest()
        )
    [request] = requests
    assert "Range" not in request.headers
    assert path.read_bytes() == MP4


async def test_cancellation_closes_all_ranges_and_cleans_the_part_file(tmp_path):
    ranges_started, ranges_closed = [], []
    all_started = asyncio.Event()
    full_requests = 0

    async def handle(request):
        nonlocal full_requests
        if "Range" not in request.headers:
            full_requests += 1
            return streamed(200, MP4, Accept_Ranges="bytes")
        start, end = requested_span(request)

        async def unfinished():
            try:
                yield MP4[start : start + 1]
                ranges_started.append(start)
                if len(ranges_started) == 4:
                    all_started.set()
                await asyncio.Future()
            finally:
                ranges_closed.append(start)

        return httpx.Response(
            206,
            content=unfinished(),
            headers={
                "Content-Range": f"bytes {start}-{end}/{len(MP4)}",
            },
        )

    path = tmp_path / "video.mp4"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        job = asyncio.create_task(download.transfer(client, MEDIA_URL, path, range_min_bytes=1))
        try:
            await asyncio.wait_for(all_started.wait(), 1)
            assert path.with_suffix(".part").is_file()
        finally:
            job.cancel()
            with pytest.raises(asyncio.CancelledError):
                await job

    assert full_requests == 1
    assert sorted(ranges_closed) == sorted(ranges_started)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("rate", "minimum", "accepted"), [(60, 20, True), (10, 20, False), (10, 8, True)]
)
async def test_slow_download_window_uses_actual_chunks(
    tmp_path, monkeypatch, rate, minimum, accepted
):
    now = 0.0
    monkeypatch.setattr(download, "time", SimpleNamespace(monotonic=lambda: now))
    body = MP4.ljust(rate * 1024 * 9, b"x")

    async def chunks():
        yield body[:4]
        for step in range(5):
            nonlocal now
            now += 9
            yield body[4:] if step == 0 else body

    response = httpx.Response(200, content=chunks(), headers={"Content-Length": str(len(body) * 5)})
    path = tmp_path / "download.mp4"
    async with media_client(response) as client:
        transfer = download.transfer(
            client, "https://v16.tiktokcdn.com/a", path, slow_bytes=minimum * 1024
        )
        if accepted:
            assert await transfer == hashlib.sha256(body * 5).hexdigest()
        else:
            with pytest.raises(Broken, match="下载太慢"):
                await transfer
    assert path.exists() is accepted
    assert not path.with_suffix(".part").exists()


@pytest.mark.parametrize("known_size", [True, False])
async def test_transfer_reports_bytes_during_each_attempt(tmp_path, known_size, monkeypatch):
    first = MP4.ljust(256 * 1024, b"x")
    last = b"remaining"
    total = len(first) + len(last) if known_size else None
    progress = download.DownloadProgress("task", "video", received_bytes=123, total_bytes=456)
    rate = download.DownloadRate()
    monkeypatch.setattr(download, "time", SimpleNamespace(monotonic=lambda: 10.2))
    observed = []

    async def chunks():
        observed.append((progress.stage, progress.received_bytes, progress.total_bytes))
        yield first
        observed.append((progress.stage, progress.received_bytes, progress.total_bytes))
        yield last

    headers = {"Content-Length": str(total)} if known_size else {"Transfer-Encoding": "chunked"}
    response = httpx.Response(200, content=chunks(), headers=headers)
    async with media_client(response) as client:
        await download.transfer(
            client, "https://v16.tiktokcdn.com/a", tmp_path / "v.mp4", progress, rate=rate
        )

    assert observed == [("downloading", 0, total), ("downloading", len(first), total)]
    assert progress.received_bytes == len(first) + len(last)
    assert progress.total_bytes == total
    assert rate.bytes_per_second(11) == (len(first) + len(last)) // 5


def test_download_rate_sums_recent_transfers_and_returns_to_zero_when_idle():
    rate = download.DownloadRate()
    rate.record(200, 10.2)
    rate.record(300, 10.7)
    rate.record(1000, 11.4)

    assert rate.bytes_per_second(10.9) == 0
    assert rate.bytes_per_second(12.0) == 300
    assert rate.bytes_per_second(16.0) == 200
    assert rate.bytes_per_second(17.0) == 0


@pytest.mark.parametrize(
    ("response", "url", "error"),
    [
        (streamed(403, b"Access Denied"), "https://v16.tiktokcdn.com/a", Stop),
        (
            streamed(200, b"<html></html>", Content_Type="text/html"),
            "https://v16.tiktokcdn.com/a",
            Skip,
        ),
        (streamed(404), "https://v16.tiktokcdn.com/a", Skip),
        (streamed(206, MP4[:10]), "https://v16.tiktokcdn.com/a", Broken),
        (
            streamed(200, MP4, Content_Length=str(len(MP4) + 5)),
            "https://v16.tiktokcdn.com/a",
            Broken,
        ),
        (streamed(200, MP4), "https://evil.example.com/a", Skip),
    ],
    ids=["拒绝", "网页", "404", "部分内容", "截断", "不受信任的域名"],
)
async def test_responses_that_are_not_a_complete_video(tmp_path, response, url, error):
    path = tmp_path / "v.mp4"
    with pytest.raises(error):
        await download.transfer(media_client(response), url, path)
    assert not path.exists()
    assert not path.with_suffix(".part").exists()


async def test_a_403_marks_the_token_as_refused(tmp_path):
    with pytest.raises(Stop) as stop:
        await download.transfer(
            media_client(streamed(403)), "https://v16.tiktokcdn.com/a", tmp_path / "v.mp4"
        )
    assert stop.value.blocked
    assert stop.value.stale
    assert stop.value.outcome == "suspect"


async def test_a_429_with_an_http_date_waits_instead_of_failing(tmp_path):
    later = format_datetime(datetime.now(UTC) + timedelta(seconds=120), usegmt=True)
    with pytest.raises(Stop) as stop:
        await download.transfer(
            media_client(streamed(429, Retry_After=later)),
            "https://v16.tiktokcdn.com/a",
            tmp_path / "v.mp4",
        )
    assert stop.value.outcome == "throttled"
    assert 118 <= stop.value.retry_after <= 120


def test_proxy_test_escapes_every_special_character_in_the_password():
    item = {"host": "10.0.0.2", "port": 1080, "username": "u", "password": "a/b@c:d"}
    url = httpx.URL(proxy_address(item, "http"))
    assert (url.host, url.username, url.password) == ("10.0.0.2", "u", "a/b@c:d")


def speed_client(status: int, body: bytes) -> httpx.Client:
    response = httpx.Response(status, content=body)
    return httpx.Client(transport=httpx.MockTransport(lambda _: response))


@pytest.mark.parametrize(
    ("status", "body", "reason"),
    [
        (206, b"01234", "测速地址返回 206"),
        (200, b"", "收到 0 字节，应为 10 字节"),
        (200, b"01234", "收到 5 字节，应为 10 字节"),
    ],
    ids=["部分内容", "空正文", "截断"],
)
def test_speed_tests_only_count_complete_files(status, body, reason):
    assert speed_download(speed_client(200, b"0" * 10), "https://speed.test/file", 10) == 10
    with pytest.raises(Incomplete, match=reason):
        speed_download(speed_client(status, body), "https://speed.test/file", 10)
