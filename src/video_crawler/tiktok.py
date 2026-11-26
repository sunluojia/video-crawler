"""TikTok 页面数据的解析和选片，规则见爬取方案第 7 节。

主页和作品页的数据在 __UNIVERSAL_DATA_FOR_REHYDRATION__ 脚本里；作品列表由页面自己请求
/api/post/item_list/。2026-11-19 实测：第一页的请求游标是 0，之后每页的请求游标等于上一页
返回的 cursor；普通作品严格按发布时间倒序，置顶作品只在第一页出现、带 isPinnedItem，
不会在后面按时间再出现一次。
"""

import json
import re
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from math import inf
from operator import itemgetter
from urllib.parse import parse_qs, urlsplit

from video_crawler.settings import MAX_VIDEO_HEIGHT

HOME = "https://www.tiktok.com/"
DATA_ID = "__UNIVERSAL_DATA_FOR_REHYDRATION__"
DATA_SCRIPT = f"document.getElementById('{DATA_ID}')?.textContent"
ITEM_LIST = "/api/post/item_list/"
USER_GONE = 10221
"""账号不存在或已封禁（statusMsg 为 user banned）。"""
ITEM_GONE = 10204
"""作品不存在（item doesn't exist）。"""
ITEM_PRIVATE = (10216, 10222)
"""私密作品、私密账号。"""
VIDEO_LOGIN_REQUIRED = "视频需登录查看"
VIDEO_PRIVATE = "视频需关注或为私密视频"


class VideoUnavailable(LookupError):
    """作品详情已经给出明确的不可用结论。"""


class NoPlaybackAddress(VideoUnavailable):
    """服务端返回了目标视频，但未提供播放地址。"""


class BlockedVideoPage(ValueError):
    """作品页有验证或访问封锁提示，可以换线路后再试。"""


_DATA = re.compile(rf'<script id="{DATA_ID}"[^>]*>(.*?)</script>', re.S)
_RESOLUTION_LABEL = re.compile(r"(?:^|[_-])([1-9][0-9]{2,3})p?(?=$|[_-])", re.IGNORECASE)


def scope(raw: str | None) -> dict | None:
    """页面数据里的 __DEFAULT_SCOPE__；没有或解析不了时返回 None，多半是停在了挑战页。"""
    try:
        data = json.loads(raw)["__DEFAULT_SCOPE__"]
    except (TypeError, ValueError, KeyError):
        return None
    return data if isinstance(data, dict) else None


def scope_in_html(html: str) -> dict | None:
    match = _DATA.search(html)
    return scope(match and match[1])


AUDIENCE_CONTROLS = "this creator turned on audience controls"
"""达人开启受众控制时主页上的提示，取自旧系统实际遇到的页面；这类达人要登录才能看作品。"""


_PROFILE_LOGIN_PROMPT = re.compile(
    r"\b(?:log ?in|sign ?in) to (?:watch|view|see) this creator's (?:videos|posts)\b"
)
_VIDEO_LOGIN_PROMPT = re.compile(
    r"\b(?:log ?in|sign ?in) to (?:watch|view|see) this (?:video|post|content)\b"
    r"|\bthis (?:video|post|content) is age[- ]restricted\b"
)
_VIDEO_PRIVATE_PROMPT = re.compile(
    r"\bthis (?:account|video|post) is private\b"
    r"|\bfollow this (?:creator|account) to (?:watch|view|see) "
    r"(?:this (?:video|post)|their (?:videos|posts|contents))\b"
)
_VIDEO_PAGE_BLOCKED = re.compile(
    r"\bcaptcha\b|\bverify (?:to continue|you(?: are|'re) human)\b"
    r"|\b(?:ip address|access) (?:is|has been) blocked\b"
)


def _notice_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.casefold().replace("’", "'").split())


def needs_login(page_text: str) -> bool:
    notice = _notice_text(page_text)
    return AUDIENCE_CONTROLS in notice or bool(_PROFILE_LOGIN_PROMPT.search(notice))


def video_access_notice(page_text: str) -> str | None:
    """识别要求登录观看或关注的提示。导航栏的登录按钮本身不足以判断。"""
    notice = _notice_text(page_text)
    if _VIDEO_PAGE_BLOCKED.search(notice):
        raise BlockedVideoPage("作品页出现验证或访问封锁提示")
    if _VIDEO_PRIVATE_PROMPT.search(notice):
        return VIDEO_PRIVATE
    if AUDIENCE_CONTROLS in notice or _VIDEO_LOGIN_PROMPT.search(notice):
        return VIDEO_LOGIN_REQUIRED
    return None


def profile_of(data: dict, handle: str) -> tuple[str | None, dict | None]:
    """主页数据里的结论和资料：结论为 private、not_found，公开账号为 None。
    需要登录（login_required）要等作品列表读不到时看页面文字才知道，见 needs_login。"""
    detail = data.get("webapp.user-detail") or {}
    user = (detail.get("userInfo") or {}).get("user")
    if not user:
        if detail.get("statusCode") == USER_GONE:
            return "not_found", None
        raise ValueError(
            f"主页没有账号资料（{detail.get('statusCode')} {detail.get('statusMsg')}）"
        )
    if user.get("uniqueId", "").casefold() != handle.casefold():
        raise ValueError(f"主页账号是 {user.get('uniqueId')}，不是 {handle}")
    profile = {
        "handle": user["uniqueId"],
        "url": f"https://www.tiktok.com/@{user['uniqueId']}",
        "nickname": user.get("nickname") or None,
        "platform_user_id": str(user["id"]) if user.get("id") else None,
    }
    return ("private" if user.get("privateAccount") else None), profile


def media_of(data: dict, work_id: str, max_video_height: int = MAX_VIDEO_HEIGHT) -> list[str]:
    """读取作品页的媒体候选，按短边限制选流。明确受限与无效页面分开处理。"""
    detail = data.get("webapp.video-detail") or {}
    if not isinstance(detail, dict):
        raise ValueError("作品页的详情格式无效")
    status = detail.get("statusCode")
    info = detail.get("itemInfo")
    item = info.get("itemStruct") if isinstance(info, dict) else None
    matches = isinstance(item, dict) and str(item.get("id")) == work_id
    if matches or item is None or item == {}:
        _check_video_access(status, detail.get("statusMsg"), item if matches else {})
    if not matches:
        raise ValueError(f"作品页没有这条视频（{status} {detail.get('statusMsg')}）")

    candidates = media_candidates(item, max_video_height)
    if not candidates and status == 0 and status is not False and _playback_withheld(item):
        kind = "挂车/受限视频" if item.get("anchors") or item.get("AnchorTypes") else "受限视频"
        raise NoPlaybackAddress(f"{kind}（服务端未返回播放地址）")
    return candidates


def _check_video_access(status: object, message: object, item: dict) -> None:
    notice = _notice_text(message).rstrip(".!。！")
    reason = video_access_notice(notice)
    if status == ITEM_GONE:
        raise VideoUnavailable("视频已删除或不存在")
    if (
        status in ITEM_PRIVATE
        or item.get("privateItem") is True
        or notice in {"private item", "item is private", "user is private", "private account"}
    ):
        raise VideoUnavailable(VIDEO_PRIVATE)
    if notice in {"login required", "login_required", "not logged in", "age restricted"}:
        raise VideoUnavailable(VIDEO_LOGIN_REQUIRED)
    if reason:
        raise VideoUnavailable(reason)


def _playback_withheld(item: dict) -> bool:
    if item.get("imagePost") is not None:
        return False
    video = item.get("video")
    if video is None:
        return True
    return (
        isinstance(video, dict)
        and video.get("playAddr") in (None, "")
        and video.get("downloadAddr") in (None, "")
        and video.get("bitrateInfo") in (None, [])
    )


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    try:
        number = int(value)
    except ValueError:
        return None
    return number if number > 0 else None


def _short_edge(address: dict) -> int | None:
    width = _positive_int(address.get("Width") or address.get("width"))
    height = _positive_int(address.get("Height") or address.get("height"))
    return min(width, height) if width and height else None


def _label_height(value: object) -> int | None:
    if isinstance(value, str) and (match := _RESOLUTION_LABEL.search(value.strip())):
        return int(match[1])
    return None


def _codec_rank(codec_type: object) -> int:
    codec = codec_type.casefold().strip() if isinstance(codec_type, str) else ""
    if codec.startswith(("h264", "avc")):
        return 0
    if not codec or codec.startswith(("h265", "hevc", "hvc1", "hev1", "bytevc1", "av1", "av01")):
        return 1
    return 2


def _https_urls(values: object) -> list[str]:
    if not isinstance(values, list):
        return []
    return [url for url in values if isinstance(url, str) and url.startswith("https://")]


def media_candidates(item: dict, max_video_height: int = MAX_VIDEO_HEIGHT) -> list[str]:
    """优先返回符合短边限制的码流。没有可用的合规地址时保留原来的回退顺序。"""
    video = item.get("video")
    if not isinstance(video, dict):
        return []
    play_url = video.get("playAddr")
    fallback = _https_urls([play_url])
    eligible = []
    rates = video.get("bitrateInfo")
    if isinstance(rates, list):
        for rate in rates:
            if not isinstance(rate, dict):
                continue
            address = rate.get("PlayAddr") or rate.get("playAddr")
            if not isinstance(address, dict):
                continue
            urls = _https_urls(address.get("UrlList") or address.get("urlList"))
            if not urls:
                continue
            fallback.extend(urls)
            height = (
                _short_edge(address)
                or _label_height(rate.get("GearName") or rate.get("gearName"))
                or _label_height(rate.get("Ratio") or rate.get("ratio"))
            )
            # 整条作品的宽高和 ratio 只能用来补足主播放地址对应的码流。
            if height is None and play_url in urls:
                height = _short_edge(video) or _label_height(video.get("ratio"))
            if height is not None and height <= max_video_height:
                rank = (
                    -height,
                    _codec_rank(rate.get("CodecType") or rate.get("codecType")),
                    _positive_int(address.get("DataSize") or address.get("dataSize")) or inf,
                    _positive_int(rate.get("Bitrate") or rate.get("bitrate")) or inf,
                )
                eligible.append((rank, urls))
    if eligible:
        ordered = (url for _, urls in sorted(eligible, key=itemgetter(0)) for url in urls)
        return list(dict.fromkeys(ordered))[:3]
    fallback.extend(_https_urls([video.get("downloadAddr")]))
    return list(dict.fromkeys(fallback))[:3]


@dataclass(frozen=True)
class MediaLinks:
    identity_id: str
    urls: tuple[str, ...]
    cached_at: float


class MediaCache:
    """本进程短时复用列表里的媒体候选。只记录身份 ID，不存 Cookie。"""

    def __init__(self, ttl: float = 8 * 60, limit: int = 2000) -> None:
        self._ttl, self._limit = ttl, limit
        self._entries: OrderedDict[str, MediaLinks] = OrderedDict()

    def remember(self, identity_id: str, work_id: str, urls: list[str]) -> None:
        now = time.monotonic()
        self._entries[work_id] = MediaLinks(identity_id, tuple(urls), now)
        self._entries.move_to_end(work_id)
        self._prune(now)

    def get(self, work_id: str) -> MediaLinks | None:
        self._prune(time.monotonic())
        return self._entries.get(work_id)

    def has_usable(self, usable_identity: Callable[[str], bool]) -> bool:
        self._prune(time.monotonic())
        return any(usable_identity(links.identity_id) for links in self._entries.values())

    def discard(self, work_id: str, links: MediaLinks) -> None:
        if self._entries.get(work_id) is links:
            del self._entries[work_id]

    def _prune(self, now: float) -> None:
        while self._entries:
            oldest = next(iter(self._entries.values()))
            if len(self._entries) <= self._limit and now - oldest.cached_at < self._ttl:
                break
            self._entries.popitem(last=False)


def request_cursor(url: str) -> str:
    return parse_qs(urlsplit(url).query).get("cursor", ["0"])[0]


@dataclass
class Listing:
    """一位达人的作品列表，按页面加载的顺序一页页加进来，够确定前几条时就结算。"""

    cutoff: datetime
    limit: int
    items: dict[str, dict] = field(default_factory=dict)
    cursors: set[str] = field(default_factory=set)
    """已读到的页返回的游标，下一页的请求游标必须在这里面，保证从第一页起没有缺页。"""
    ended: bool = False
    oldest: int | None = None
    """读到的最早的普通作品的发布时间（秒）。"""
    on_media: Callable[[str, list[str]], None] | None = None
    max_video_height: int = MAX_VIDEO_HEIGHT

    def add(self, cursor: str, page: dict) -> bool:
        """加一页。请求游标接不上已读页的不收，保证从第一页起没有缺页；
        页面重载后再请求一次第一页没关系，作品按 ID 去重。"""
        if cursor != "0" and cursor not in self.cursors:
            return False
        if not isinstance(page, dict) or page.get("statusCode", 0) not in (0, "0"):
            raise ValueError("作品列表返回失败状态或非对象")
        more, items = page.get("hasMore"), page.get("itemList")
        if not isinstance(more, (bool, int)) or more not in (0, 1):
            raise ValueError("作品列表缺少有效的 hasMore")
        if items is None and not more and ("itemList" in page or "statusCode" in page):
            items = []
        if not isinstance(items, list):
            raise ValueError("作品列表缺少有效的 itemList")
        next_cursor = page.get("cursor")
        if more and (next_cursor is None or next_cursor == ""):
            raise ValueError("作品列表有下一页，但没有游标")
        for item in items:
            work_id = str(item["id"])
            self.items[work_id] = item
            if not item.get("isPinnedItem"):
                created = int(item["createTime"])
                self.oldest = created if self.oldest is None else min(self.oldest, created)
            if self.on_media and not item.get("imagePost"):
                urls = media_candidates(item, self.max_video_height)
                if urls:
                    self.on_media(work_id, urls)
        self.cursors.add(str(next_cursor))
        self.ended = self.ended or not more
        return True

    @property
    def started(self) -> bool:
        return bool(self.cursors)

    def _eligible(self) -> Iterator[dict]:
        cutoff = self.cutoff.timestamp()
        return (item for item in self.items.values() if int(item["createTime"]) <= cutoff)

    @property
    def candidate_count(self) -> int:
        return min(self.limit, sum(1 for _ in self._eligible()))

    def _ranked(self) -> list[dict]:
        return sorted(
            self._eligible(), key=lambda item: (-int(item["createTime"]), -int(item["id"]))
        )

    @property
    def settled(self) -> bool:
        """读到末页，或者已经读到一条严格早于第 limit 名的普通作品（同一秒的要继续读）。"""
        if self.ended:
            return True
        ranked = self._ranked()
        return (
            len(ranked) >= self.limit
            and self.oldest is not None
            and self.oldest < int(ranked[self.limit - 1]["createTime"])
        )

    def works(self) -> list[dict]:
        """前 limit 条，按调度中心的作品字段；图文和视频一起占位。"""
        return [
            {
                "position": position,
                "work_id": str(item["id"]),
                "url": f"https://www.tiktok.com/@{item['author']['uniqueId']}/"
                f"{'photo' if item.get('imagePost') else 'video'}/{item['id']}",
                "type": "photo" if item.get("imagePost") else "video",
                "published_at": datetime.fromtimestamp(int(item["createTime"]), UTC).isoformat(),
                "description": item.get("desc") or "",
                "topics": [
                    tag["title"] for tag in item.get("challenges") or [] if tag.get("title")
                ],
            }
            for position, item in enumerate(self._ranked()[: self.limit], 1)
        ]
