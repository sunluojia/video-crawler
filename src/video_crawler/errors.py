"""一次尝试没做成时怎么办，对应爬取方案第 7 节和单视频下载方案第 3 节的三类失败。"""


class Wait(Exception):
    """环境问题：申请不到线路、没有可用身份、磁盘满……过一会儿再试，不算尝试次数。"""

    def __init__(self, seconds: float, reason: str, use: str | None = None) -> None:
        super().__init__(reason)
        self.seconds, self.reason = seconds, reason
        self.use = use
        """在等哪个用途的线路；下载在等采集线路时，先不开始新的获取作品。"""


class Retry(Exception):
    """这次没做成：算一次尝试，稍后避开出问题的线路再试。"""

    def __init__(self, reason: str, line: str | None = None) -> None:
        super().__init__(reason)
        self.reason, self.line = reason, line


class Gone(Exception):
    """内容本身的结论，例如视频已删除或设为私密：再试也没用，直接结束。"""
