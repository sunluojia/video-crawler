"""调度中心给的线路怎么连。

HTTP 请求（curl_cffi、httpx）直接连代理，域名交给代理解析（socks5h）。浏览器不能直接用带口令的代理：
带口令的 HTTP 代理报 ERR_INVALID_AUTH_CREDENTIALS，带口令的 SOCKS5 报 ERR_NO_SUPPORTED_PROXIES。
所以浏览器每次启动时在本机开一个无口令的转发口，端口由系统分配，浏览器关了就关掉。
"""

import asyncio
import contextlib
from base64 import b64encode
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import SplitResult, quote, unquote, urlsplit

from video_crawler.errors import Retry


async def proxy_url(route: dict) -> str | None:
    """HTTP 客户端用的代理地址；直连返回 None。"""
    if route["kind"] == "direct":
        return None
    host = route.get("host") or "127.0.0.1"  # 本机端口没有主机
    protocol = route.get("protocol") or "auto"
    if protocol == "ssh":
        raise Retry("爬取程序暂不支持 SSH 代理")
    if protocol == "auto":  # 本机端口不写协议：先问一句 SOCKS5，不回应就按 HTTP
        protocol = "socks5" if await socks5_answers(host, route["port"]) else "http"
    scheme = "socks5h" if protocol == "socks5" else protocol
    auth = (
        f"{quote(route['username'], safe='')}:{quote(route['password'], safe='')}@"
        if route.get("username")
        else ""
    )
    netloc = f"[{host}]" if ":" in host else host
    return f"{scheme}://{auth}{netloc}:{route['port']}"


async def socks5_answers(host: str, port: int) -> bool:
    """发 SOCKS5 问候，几秒内按 SOCKS5 回应才算。"""
    try:
        async with asyncio.timeout(5):
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(b"\x05\x02\x00\x02")
            answer = await reader.read(2)
            writer.close()
    except (OSError, TimeoutError):
        return False
    return answer[:1] == b"\x05"


@asynccontextmanager
async def browser_proxy(route: dict) -> AsyncIterator[dict | None]:
    """浏览器用的代理设置；带口令的代理先在本机开一个无口令的转发口。"""
    url = await proxy_url(route)
    upstream = urlsplit(url or "")
    if not url or not upstream.username:
        yield url and {"server": url.replace("socks5h://", "socks5://", 1)}
        return
    # 每条连接的任务自己存着：浏览器先断开后 asyncio 就不再引用它，还在等上游的会被当垃圾回收
    connections: set[asyncio.Task] = set()

    def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(_forward(upstream, reader, writer))
        connections.add(task)
        task.add_done_callback(connections.discard)

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    try:
        yield {"server": f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"}
    finally:
        server.close()
        for task in connections:
            task.cancel()
        await asyncio.gather(*connections, return_exceptions=True)


async def _forward(
    upstream: SplitResult, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """只转发 CONNECT：浏览器访问的都是 https。结束或被取消时两头的连接都关掉。"""
    with contextlib.closing(writer):
        try:
            method, target, _ = (await reader.readuntil(b"\r\n\r\n")).split(b" ", 2)
            if method != b"CONNECT":
                raise ConnectionError("只转发 CONNECT")
            host, _, port = target.decode().rpartition(":")
            remote_reader, remote_writer = await _dial(upstream, host.strip("[]"), int(port))
        except (OSError, ValueError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            return
        with contextlib.closing(remote_writer):
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await asyncio.gather(_pipe(reader, remote_writer), _pipe(remote_reader, writer))


async def _dial(upstream: SplitResult, host: str, port: int):
    """经带口令的上游代理连到目标；握手没成（包括被取消）就关掉这条上游连接。"""
    reader, writer = await asyncio.open_connection(
        upstream.hostname, upstream.port, ssl=upstream.scheme == "https" or None
    )
    try:
        await _handshake(upstream, host, port, reader, writer)
    except BaseException:
        writer.close()
        raise
    return reader, writer


async def _handshake(
    upstream: SplitResult,
    host: str,
    port: int,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    user, password = unquote(upstream.username or ""), unquote(upstream.password or "")
    if upstream.scheme.startswith("socks5"):
        writer.write(b"\x05\x01\x02")
        if (await reader.readexactly(2))[1] != 2:
            raise ConnectionError("SOCKS5 代理不接受口令认证")
        name, secret = user.encode(), password.encode()
        writer.write(bytes([1, len(name)]) + name + bytes([len(secret)]) + secret)
        if (await reader.readexactly(2))[1] != 0:
            raise ConnectionError("SOCKS5 认证失败")
        target = host.encode()
        writer.write(b"\x05\x01\x00\x03" + bytes([len(target)]) + target + port.to_bytes(2, "big"))
        reply = await reader.readexactly(4)
        if reply[1] != 0:
            raise ConnectionError(f"SOCKS5 连接目标失败（{reply[1]}）")
        length = {1: 4, 4: 16}.get(reply[3]) or (await reader.readexactly(1))[0]
        await reader.readexactly(length + 2)
        return
    token = b64encode(f"{user}:{password}".encode()).decode()
    writer.write(
        f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n"
        f"Proxy-Authorization: Basic {token}\r\n\r\n".encode()
    )
    status = (await reader.readuntil(b"\r\n\r\n")).split(b"\r\n", 1)[0]
    if status.split(b" ")[1:2] != [b"200"]:
        raise ConnectionError(status.decode(errors="replace"))


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    with contextlib.suppress(OSError):
        while chunk := await reader.read(65536):
            writer.write(chunk)
            await writer.drain()
    writer.close()
