# 爬取程序

向[调度中心](https://github.com/sunluojia/video-scheduler-center)认领任务，获取 TikTok 达人资料和最新五条作品（`crawl_creator`），或下载视频并上传（`download_video`）。

## 怎么工作

- **认领与续租**　一个进程使用稳定的 `VC_INSTANCE_ID` 和本次启动生成的会话 UUID，向调度中心认领 PostgreSQL 中的任务。采集名额受空闲身份数约束。存在空闲身份或可用缓存时，下载可领满空槽。只剩一个空闲身份时，采集与下载轮流优先认领。每 15 秒报告在途尝试和线路。收到撤销时停止对应尝试。
- **采集线路**　`crawler.crawl_creator` 用于预热身份、读取达人资料和作品列表，以及下载前为单条作品取媒体地址。取链先用 HTTP，遇到挑战页或缺少作品数据时用同一身份和线路打开 Chrome。拿到地址后立即归还采集租约。
- **下载线路**　`crawler.download_video` 用于拉取 MP4。取链后归还身份，拉流使用同一身份的 UA、Referer 和 `tt_chain_token` 快照。从作品列表取到的直链只保留在本进程内存，最多 2,000 条，8 分钟有效。同身份的会话仍有效时，采集期间也可直接用缓存下载。缓存直链收到 403 或所有候选不可用时，清掉旧条目再取链。每条线路分别回报结果，文件校验后携带尝试和会话归属上传。
- **重试与结果**　线路、身份、磁盘、限流或代理 407 等环境等待会退回调度中心，不扣业务次数。业务失败按约 1 分钟、5 分钟退避，最多尝试三次。结果投递到 Redis `crawler.events`。入队前遇到 Redis 暂时不可用，进程会续租并用同一个事件 ID 重投。进程在入队前退出时，调度中心可在租约过期后重派任务。
- **身份与文件**　身份元数据按设备保存在 Redis。Cookie 由本机 Chrome 资料目录管理，媒体直链和会话 Cookie 不发送到 Redis。下载临时文件按尝试隔离。

## 运行

需要 [uv](https://docs.astral.sh/uv/)、本机 Chrome、支持任务认领协议的调度中心，以及调度中心的 Redis。身份预热默认用无头 Chrome（`VC_WARM_HEADLESS=1`）。采集和作品页的 Chrome 回退需要图形环境，Linux 可用 Xvfb。

```bash
cp .env.example .env
```

在 `.env` 里填 `VC_SERVICE_TOKEN`、调度中心地址和 Redis 地址，然后：

```bash
uv sync
```

```bash
uv run video-crawler
```

在调度中心的「代理」为采集用途配置美国住宅线路，为下载用途配置香港直连。采集、下载、预热并发由「执行程序」设置，身份目标由「账号池」设置，调整随心跳生效。下载低速断开条件默认是 45 秒内平均低于 20 KiB/s，可用 `VC_DOWNLOAD_SLOW_BYTES`（字节/秒）和 `VC_DOWNLOAD_SLOW_WINDOW`（秒）调整。

声明支持字节范围并达到 1 MiB 的视频默认用 4 条并行 HTTP Range 拉取，各分片须返回 206 且范围、长度吻合。分片失败会重新用单流下载。`VC_DOWNLOAD_RANGE_PARTS=1` 可关闭分片，`VC_DOWNLOAD_RANGE_MIN_BYTES` 可调整最小文件字节数。

下载默认优先选短边不超过 720 像素的最高可用档位，横屏和竖屏适用同一规则。同一档位优先 H.264，再按文件大小和码率排序。如果没有可用的合规码流，按原播放地址、码流地址和下载地址回退。可用 `CRAWLER_MAX_VIDEO_HEIGHT` 调整短边上限。

控制接口默认监听 8091，使用服务凭据鉴权。`/api/health` 检查进程存活，`/api/status` 显示执行状态，`/api/progress` 合并采集与下载进度。`/api/batches/{id}/pause` 停止该批次的在途采集并归还尝试，下载继续执行。`resume` 恢复采集，`cancel` 停止该批次的所有在途尝试。身份操作和代理测试接口继续提供。

`data/` 保存下载中的文件，`profiles/` 保存本机 Chrome 资料。两个目录均在 Git 忽略列表内。

## 测试

```bash
uv run --extra dev python -m pytest -q
```

```bash
uv run --extra dev ruff check src tests
```
