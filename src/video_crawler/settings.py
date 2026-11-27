"""运行参数，从环境变量或 .env 读取。"""

import socket
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

SLOW_BYTES, SLOW_WINDOW = 20 * 1024, 45
RANGE_PARTS, RANGE_MIN_BYTES = 4, 1024 * 1024
MAX_VIDEO_HEIGHT = 720
type BlockableResource = Literal["image", "media", "font"]
BLOCKED_RESOURCE_TYPES: tuple[BlockableResource, ...] = ("image", "media", "font")
BROWSER_CONTEXT_MAX_USES, BROWSER_CONTEXT_IDLE_TTL_SECONDS = 30, 600
BROWSER_CONTEXT_POOL_SIZE = 6


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VC_", env_file=".env", extra="ignore")

    service_token: SecretStr
    """与调度中心、分析服务相同的服务凭据。"""
    scheduler_url: str = "http://127.0.0.1:8080"
    redis_url: str = "redis://localhost:6379/0"
    instance_id: str = Field(default_factory=lambda: f"crawler-{socket.gethostname()}")
    """设备标识：注册、申请线路都用它。"""
    host: str = "127.0.0.1"
    port: int = 8091
    public_url: str | None = None
    """调度中心访问本程序的地址，默认 http://<host>:<port>。"""
    data_dir: Path = Path("data")
    """下载中的本地文件。"""
    profile_root: Path = Path("profiles")
    """身份的浏览器资料目录。里面有 Cookie，按密码对待，不进仓库。"""
    crawl_policy_enabled: bool = False
    crawl_defer_max_seconds: int = Field(default=30, ge=1, le=30)
    crawl_empty_session_hold_seconds: int = Field(default=180, ge=0)
    crawl_warm_recovery_min_seconds: int = Field(
        default=90,
        ge=0,
        validation_alias=AliasChoices(
            "VC_CRAWL_WARM_RECOVERY_MIN_SECONDS",
            "VC_WARM_MIN_INTERVAL_SECONDS",
            "crawl_warm_recovery_min_seconds",
        ),
    )
    keep_profile_dirs: bool = False
    download_slow_bytes: int = Field(default=SLOW_BYTES, gt=0)
    """下载断开阈值，单位是字节每秒。"""
    download_slow_window: float = Field(default=SLOW_WINDOW, gt=0, allow_inf_nan=False)
    """慢速下载的统计窗口和无数据读取超时，单位是秒。"""
    download_range_parts: int = Field(default=RANGE_PARTS, ge=1, le=4)
    """并发 HTTP Range 分片数，设为 1 时使用单流。"""
    download_range_min_bytes: int = Field(default=RANGE_MIN_BYTES, gt=0)
    """至少多少字节才试分片。"""
    max_video_height: int = Field(
        default=MAX_VIDEO_HEIGHT,
        gt=0,
        validation_alias=AliasChoices("CRAWLER_MAX_VIDEO_HEIGHT", "max_video_height"),
    )
    """优先下载的最高分辨率，横屏和竖屏均按短边计算。"""
    preextract_media: bool = False
    preextract_frame_count: int = Field(default=6, ge=1, le=1000)
    preextract_audio_segment_seconds: int = Field(default=60, gt=0)
    preextract_concurrency: int = Field(default=4, gt=0)
    ffmpeg_path: str = "ffmpeg"
    ffprobe_path: str = "ffprobe"
    chrome_channel: str = "chrome"
    block_nonessential_resources: bool = True
    blocked_resource_types: tuple[BlockableResource, ...] = BLOCKED_RESOURCE_TYPES
    reuse_browser_context: bool = True
    browser_context_pool_size: int = Field(default=BROWSER_CONTEXT_POOL_SIZE, ge=0)
    browser_context_max_uses: int = Field(default=BROWSER_CONTEXT_MAX_USES, gt=0)
    browser_context_idle_ttl_seconds: float = Field(
        default=BROWSER_CONTEXT_IDLE_TTL_SECONDS, gt=0, allow_inf_nan=False
    )
    shared_media_cache: bool = True
    warm_headless: bool = True
    """预热身份时用无头 Chrome（默认开）。只影响拿 Cookie 那一步：实测无头 6 秒就能拿到
    tt_chain_token，用无头预热出来的资料目录之后用有头采集完全正常（32 条，与有头预热一致）。
    采集作品必须保留有界面——同一资料目录、同一条线路，无头拿作品接口返回 403。"""

    @property
    def preserve_profiles(self) -> bool:
        return self.crawl_policy_enabled or self.keep_profile_dirs

    @property
    def url(self) -> str:
        return self.public_url or f"http://{self.host}:{self.port}"
