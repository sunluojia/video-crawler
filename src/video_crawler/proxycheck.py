"""调度中心请本机代为测试和测速代理，见代理方案第 1.5 节。逻辑与调度中心的模拟爬取程序一致。"""

import socket
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

import httpx


def socks5_answers(host: str, port: int) -> bool:
    """发 SOCKS5 问候，几秒内按 SOCKS5 回应才算。httpx 的 SOCKS 握手不受超时控制，
    遇到不回应问候的 HTTP 代理会一直等，所以先自己探一下。"""
    try:
        with socket.create_connection((host, port), timeout=5) as conn:
            conn.sendall(b"\x05\x02\x00\x02")
            return conn.recv(2)[:1] == b"\x05"
    except OSError:
        return False


def proxy_address(item: dict, protocol: str) -> str:
    """代理地址；主机为空表示本机端口，IPv6 主机加方括号。"""
    plain = item["host"] or "127.0.0.1"
    host = f"[{plain}]" if ":" in plain else plain
    user, password = quote(item["username"] or "", safe=""), quote(item["password"] or "", safe="")
    auth = f"{user}:{password}@" if item["username"] else ""
    return f"{protocol}://{auth}{host}:{item['port']}"


def check_proxy(item: dict, echo_url: str) -> dict:
    """经代理请求回显地址。连不上、握手或认证失败才算不通；握手之后失败算回显服务的问题。"""
    reply = {"key": item["key"]}
    if item["protocol"] == "ssh":
        return reply | {"result": "unreachable", "error": "爬取程序暂不支持 SSH 代理"}
    if refresh_url := item.get("refresh_url"):
        try:
            httpx.get(refresh_url, timeout=15).raise_for_status()
        except httpx.HTTPError as error:
            return reply | {"result": "unreachable", "error": f"刷新出口失败：{error}"}
    protocols = ["socks5", "http"] if item["protocol"] == "auto" else [item["protocol"]]
    failure = ""
    if "socks5" in protocols and not socks5_answers(item["host"] or "127.0.0.1", item["port"]):
        protocols.remove("socks5")
        failure = "SOCKS5 握手没有回应"
    for protocol in protocols:
        started = time.monotonic()
        try:
            with httpx.Client(proxy=proxy_address(item, protocol), timeout=15) as client:
                response = client.get(echo_url)
        except httpx.ProxyError as error:
            if "407" in str(error):  # 回显地址是 https 时，407 出现在建立隧道时
                return reply | {"result": "auth_failed", "protocol": protocol, "error": str(error)}
            failure = f"{protocol} 握手失败：{error}"
            continue
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            failure = f"连不上代理：{error}"
            continue
        except httpx.HTTPError as error:
            return reply | {"result": "echo_failed", "protocol": protocol, "error": str(error)}
        if response.status_code == 407:  # 回显地址是 http 时，代理直接回 407
            return reply | {
                "result": "auth_failed",
                "protocol": protocol,
                "error": "代理认证失败（407）",
            }
        if response.is_error:
            return reply | {
                "result": "echo_failed",
                "protocol": protocol,
                "error": f"回显服务返回 {response.status_code}",
            }
        return reply | {
            "result": "ok",
            "protocol": protocol,
            "exit_ip": response.text.strip(),
            "latency_ms": int((time.monotonic() - started) * 1000),
        }
    return reply | {"result": "unreachable", "error": failure}


class Incomplete(Exception):
    """测速文件没下完整：不是 200、空的、网页或者被截断。"""


def download(client: httpx.Client, url: str, size: int) -> int:
    """完整下载一次测速文件。只认 200，收到的字节数要和预期一致。"""
    with client.stream("GET", url) as response:
        if response.status_code != 200:
            raise Incomplete(f"测速地址返回 {response.status_code}")
        received = sum(len(chunk) for chunk in response.iter_bytes())
    if received != size:
        raise Incomplete(f"收到 {received} 字节，应为 {size} 字节")
    return received


def mbps(size: int, started: float) -> float:
    return round(size * 8 / (time.monotonic() - started) / 1e6, 1)


def measure_speed(body: dict) -> dict:
    """先单路下载一次，再几路同时下载；速度按总字节数除以从发起到全部下完的时间。"""
    item, url, size, streams = body["item"], body["url"], body["size"], body["streams"]
    proxy = proxy_address(item, item["protocol"])

    def once(_: int = 0) -> int:
        headers = {"Accept-Encoding": "identity"}
        with httpx.Client(proxy=proxy, timeout=30, headers=headers) as client:
            return download(client, url, size)

    reply = {"key": item["key"]}
    try:
        started = time.monotonic()
        single = mbps(once(), started)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=streams) as pool:
            total = sum(pool.map(once, range(streams)))
        return reply | {"single_mbps": single, "parallel_mbps": mbps(total, started)}
    except Incomplete as error:
        return reply | {"error": str(error)}
    except httpx.HTTPError as error:
        return reply | {"error": f"没下完整：{str(error) or type(error).__name__}"}
