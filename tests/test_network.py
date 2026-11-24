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


@pytest.mark.parametrize("known_size", [True, False])
async def test_transfer_reports_bytes_during_each_attempt(tmp_path, known_size, monkeypatch):
    first = MP4.ljust(download.CHUNK, b"x")
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
