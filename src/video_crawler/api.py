"""调度中心访问本程序的接口，都要求服务凭据。见调度中心设计文档第 4.4 节和代理方案第 6 节。"""

import secrets
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from video_crawler.download import DownloadProgress
from video_crawler.identities import IdentityPool
from video_crawler.proxycheck import check_proxy, measure_speed
from video_crawler.runner import CRAWL_CREATOR, DOWNLOAD_VIDEO, Runner
from video_crawler.scheduler import Scheduler
from video_crawler.store import Store


def control_api(
    token: str,
    store: Store,
    runner: Runner,
    pool: IdentityPool,
    scheduler: Scheduler,
    downloads: Callable[[], list[DownloadProgress]] = list,
    download_speed: Callable[[], int] | None = None,
) -> FastAPI:
    def authorized(
        credentials: Annotated[
            HTTPAuthorizationCredentials | None, Depends(HTTPBearer(auto_error=False))
        ],
    ) -> None:
        if not secrets.compare_digest(credentials.credentials if credentials else "", token):
            raise HTTPException(401, "服务凭据无效")

    app = FastAPI(title="爬取程序", dependencies=[Depends(authorized)])
    # 控制接口用 async，和任务队列在同一个事件循环里；测试代理要等网络，放在线程里跑

    @app.post("/api/batches/{batch_id}/{action}", status_code=204)
    async def control_batch(batch_id: str, action: str) -> None:
        """暂停只是不开始新的尝试；取消关掉进行中的尝试，之后收到的任务也丢掉。"""
        if action == "pause":
            store.pause(batch_id)
            runner.forget(batch_id)
        elif action == "resume":
            store.resume(batch_id)
            runner.wake()
        elif action == "cancel":
            store.cancel_batch(batch_id)
            runner.cancel(batch_id=batch_id)
        else:
            raise HTTPException(404)

    @app.post("/api/tasks/{task_id}/cancel", status_code=204)
    async def cancel_task(task_id: str) -> None:
        store.cancel_task(task_id)
        runner.cancel(task_id=task_id)

    @app.post("/api/identities/{identity_id}/{action}", status_code=204)
    async def control_identity(identity_id: str, action: Literal["refresh", "retire"]) -> None:
        """账号池页面的操作：刷新立即重新预热；退休删掉资料目录，后台会补一个新的。"""
        identity = pool.identities.get(identity_id)
        if identity is None:
            raise HTTPException(404, "没有这个身份，可能已经退休")
        if identity.busy:
            raise HTTPException(409, "这个身份正在使用或预热，稍后再试")
        if action == "refresh":
            pool.refresh(identity)
        else:
            pool.retire(identity, "在账号池页面手动退休")

    @app.get("/api/status")
    async def status() -> dict:
        """运行状态，调度中心“执行程序”页显示，格式见调度中心设计文档第 4.4 节。"""
        unfinished = store.unfinished()

        def jobs(kind: str) -> dict:
            running = runner.busy(kind)
            return {"running": running, "queued": unfinished.get(kind, 0) - running}

        paused = runner.paused_until > time.time()
        return {
            "crawl": jobs(CRAWL_CREATOR),
            "download": jobs(DOWNLOAD_VIDEO),
            "identities": pool.summary(),
            "leases": len(scheduler.leases),
            "paused_batches": store.paused(),
            "download_paused_until": (
                datetime.fromtimestamp(runner.paused_until, UTC).isoformat() if paused else None
            ),
        }

    @app.get("/api/download-progress")
    async def download_progress(response: Response) -> list[DownloadProgress]:
        """当前正在处理的下载，按实际接收的字节数报告。"""
        if download_speed is not None:
            response.headers["X-Download-Bytes-Per-Second"] = str(download_speed())
        return downloads()

    @app.post("/api/proxy-tests")
    def proxy_tests(body: dict) -> list[dict]:
        """调度中心请本机代为测试一批代理，并行进行。"""
        with ThreadPoolExecutor(max_workers=10) as executor:
            return list(
                executor.map(lambda item: check_proxy(item, body["echo_url"]), body["items"])
            )

    speed_lock = threading.Lock()

    @app.post("/api/proxy-speed")
    def proxy_speed(body: dict) -> dict:
        """经代理下载测速文件。本机同时只测一个，免得几个测速互相抢带宽。"""
        with speed_lock:
            return measure_speed(body)

    return app
