# 爬取程序

接收[调度中心](https://github.com/sunluojia/video-scheduler-center)的任务，获取 TikTok 达人资料和最新五条作品（`crawl_creator`），下载一条视频并上传给调度中心（`download_video`）。实现细节见[爬取方案](https://github.com/sunluojia/video-scheduler-center/blob/main/docs/crawler-design.md)和[单视频下载方案](https://github.com/sunluojia/video-scheduler-center/blob/main/docs/video-download-design.md)，接口约定见[调度中心设计文档第 4 节](https://github.com/sunluojia/video-scheduler-center/blob/main/docs/design.md#4-与爬取程序的约定)。

## 怎么工作

- **身份池**：每个身份是一个本机 Chrome 的浏览器资料目录，用它打开 TikTok 首页预热出 Cookie 和 `tt_chain_token`。在调度中心的「账号池」设置可用身份目标（默认 8 个，多台设备平均分摊），在「执行程序」按设备设置同时预热的数量（默认 1 个）。这两项随心跳下发。后台自动补充身份，满 25 分钟刷新，下载连续被拒 6 次就换掉。重启后按已有资料目录重新预热。
- **获取作品**：挑一个身份，向调度中心要一条“获取信息”线路，用身份的浏览器打开达人主页，读页面数据里的资料，抓页面自己加载的作品列表，选出截止时间前的前五条（图文和视频一起数）。
- **下载**：要一条“下载视频”线路；挑一个身份，经采集线路带它的 Cookie 读作品页拿到媒体地址；再带这个身份的 `tt_chain_token` 经下载线路下载，校验完整后上传。
- **失败**：这次没做成就 1 分钟、5 分钟后换线路再试，3 次都没成回传原因；申请不到线路、没有可用身份、磁盘满这类环境问题只是等着，不算次数。
- **线路**：都向调度中心申请，用完归还并报告表现。浏览器不能直接用带口令的代理，程序会在本机开一个无口令的转发口给它，浏览器关了就关掉。暂不支持 SSH 类型的代理。

一个进程里跑：收任务的 Dramatiq 线程（只记进本地状态表）、任务队列、身份池、事件投递、心跳和控制接口。

## 运行

需要 [uv](https://docs.astral.sh/uv/)、本机安装的 Chrome，以及调度中心的 Redis。**身份预热默认用无头 Chrome**（`VC_WARM_HEADLESS=1`），只拿 Cookie 那一步不开界面；**采集作品仍然要有界面**，Linux 上需要虚拟显示（例如 Xvfb）。

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

启动后向调度中心注册，心跳带上身份池的快照（不含 Cookie），调度中心“账号池”页面据此显示。同时获取几个达人（开几个 Chrome 窗口，默认 2）、同时下载几条（默认 4）在调度中心“执行程序”页设置，随心跳回复下发，改了不用重启。在 8091 端口提供控制接口（暂停、继续、取消批次，刷新和退休身份，`/api/status`，代为测试和测速代理）。获取作品要能访问 TikTok 的采集线路，请在调度中心“代理”页给“获取信息”配好美国住宅代理，或者给这台设备设本机端口例外。不要和模拟爬取程序同时连同一个调度中心，否则任务会被两边分走。

`data/` 里是本地状态表和下载中的文件，`profiles/` 里是身份的浏览器资料目录，都不要放进仓库，`profiles/` 等同密码。

## 测试

```bash
uv run --extra dev python -m pytest -q
```

```bash
uv run --extra dev ruff check src tests
```
