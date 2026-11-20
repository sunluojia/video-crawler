"""调度中心的接口：申请和归还线路、上传视频、注册心跳。约定见调度中心设计文档第 4 节和代理方案。"""

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import httpx

from video_crawler.errors import Wait
from video_crawler.settings import Settings
from video_crawler.store import Task

log = logging.getLogger(__name__)

CRAWL, DOWNLOAD = "crawler.crawl_creator", "crawler.download_video"
"""申请线路的用途：获取信息、下载视频。"""
USE_NAMES = {CRAWL: "采集", DOWNLOAD: "下载"}


@dataclass
class Lease:
    id: str
    line: str
    """线路标识；失败后放进 avoid，重试时避开。"""
    route: dict
    """怎么连：kind 为 proxy、direct 或 local_port，代理时带协议、地址和凭据。"""


class Scheduler:
    def __init__(self, settings: Settings) -> None:
        self.instance = settings.instance_id
        self._http = httpx.AsyncClient(
            base_url=settings.scheduler_url,
            headers={"Authorization": f"Bearer {settings.service_token.get_secret_value()}"},
            timeout=60,
        )
        self.leases: set[str] = set()
        """正在用的线路，随心跳续期。"""

    async def lease(self, use: str, task_id: str | None = None, avoid: list[str] = ()) -> Lease:
        """申请一条线路；分不到时抛 Wait，按调度中心给的时间再来。"""
        body = {"device": self.instance, "use": use, "task_id": task_id, "avoid": list(avoid)}
        try:
            response = await self._http.post("/api/proxy-leases", json=body)
            reply = response.raise_for_status().json()
        except httpx.HTTPError as error:
            raise Wait(30, f"调度中心连不上：{error}") from error
        if not (grant := reply["lease"]):
            raise Wait(
                reply["retry_after"] or 30, f"没有{USE_NAMES[use]}线路：{reply['reason']}", use
            )
        self.leases.add(grant["lease_id"])
        return Lease(grant["lease_id"], grant["line"], grant["route"])

    async def release(
        self, lease: Lease, outcome: str, detail: str | None = None, retry_after: int | None = None
    ) -> None:
        """归还线路并报告表现：normal、suspect、proxy_down、auth_failed、throttled。"""
        self.leases.discard(lease.id)
        body = {"outcome": outcome, "detail": detail and detail[:500], "retry_after": retry_after}
        try:
            response = await self._http.post(f"/api/proxy-leases/{lease.id}/release", json=body)
            response.raise_for_status()
        except httpx.HTTPError as error:
            log.warning("归还线路失败，调度中心 60 秒后自动收回：%s", error)

    async def upload(self, task: Task, path: Path, sha256: str) -> int:
        """上传一条视频，返回状态码：204 已登记，404、409 不要了，422 内容对不上。"""

        async def chunks() -> AsyncIterator[bytes]:
            with path.open("rb") as file:
                while chunk := file.read(1 << 20):
                    yield chunk

        response = await self._http.put(
            f"/api/download-tasks/{task.task_id}/runs/{task.run}/video",
            content=chunks(),
            headers={"Content-Length": str(path.stat().st_size), "X-Content-SHA256": sha256},
            timeout=600,
        )
        return response.status_code

    async def register(self, url: str, version: str, identities: dict) -> dict:
        """注册兼心跳，同时为正在用的线路续期，并带上身份池的快照。
        返回调度中心的回复：分给本机的可用身份数、获取作品和下载的并发数。"""
        body = {
            "url": url,
            "version": version,
            "leases": sorted(self.leases),
            "identities": identities,
        }
        response = await self._http.put(f"/api/crawler-services/{self.instance}", json=body)
        return response.raise_for_status().json()

    async def withdraw(self) -> None:
        await self._http.delete(f"/api/crawler-services/{self.instance}")
        await self._http.aclose()
