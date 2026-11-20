"""运行参数，从环境变量（前缀 VC_）或 .env 读取。"""

import socket
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    """本地状态表和下载中的文件。"""
    profile_root: Path = Path("profiles")
    """身份的浏览器资料目录。里面有 Cookie，按密码对待，不进仓库。"""
    chrome_channel: str = "chrome"
    warm_headless: bool = True
    """预热身份时用无头 Chrome（默认开）。只影响拿 Cookie 那一步：实测无头 6 秒就能拿到
    tt_chain_token，用无头预热出来的资料目录之后用有头采集完全正常（32 条，与有头预热一致）。
    采集作品必须保留有界面——同一资料目录、同一条线路，无头拿作品接口返回 403。"""

    @property
    def url(self) -> str:
        return self.public_url or f"http://{self.host}:{self.port}"
