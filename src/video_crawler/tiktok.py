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
from urllib.parse import parse_qs, urlsplit

HOME = "https://www.tiktok.com/"
DATA_ID = "__UNIVERSAL_DATA_FOR_REHYDRATION__"
DATA_SCRIPT = f"document.getElementById('{DATA_ID}')?.textContent"
ITEM_LIST = "/api/post/item_list/"
USER_GONE = 10221
"""账号不存在或已封禁（statusMsg 为 user banned）。"""
ITEM_GONE = 10204
"""作品不存在（item doesn't exist）。"""

_DATA = re.compile(rf'<script id="{DATA_ID}"[^>]*>(.*?)</script>', re.S)


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


def needs_login(page_text: str) -> bool:
    return AUDIENCE_CONTROLS in " ".join(page_text.casefold().split())


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


def media_of(data: dict, work_id: str) -> list[str]:
    """作品页数据里的媒体地址候选：主播放地址、各码率地址、下载地址，去重后最多 3 个。

    作品不存在时抛 LookupError，其他读不到的情况抛 ValueError。"""
    detail = data.get("webapp.video-detail") or {}
    if not isinstance(detail, dict):
        raise ValueError("作品页的详情格式无效")
    if detail.get("statusCode") == ITEM_GONE:
        raise LookupError("视频已删除或不存在")
    info = detail.get("itemInfo")
    item = info.get("itemStruct") if isinstance(info, dict) else None
    if not isinstance(item, dict) or str(item.get("id")) != work_id:
        raise ValueError(
            f"作品页没有这条视频（{detail.get('statusCode')} {detail.get('statusMsg')}）"
        )
    return media_candidates(item)


def media_candidates(item: dict) -> list[str]:
    """单条作品和列表共用同一套媒体字段，忽略缺失或无效的可选字段。"""
    video = item.get("video")
    if not isinstance(video, dict):
        return []
    urls = [video.get("playAddr")]
    rates = video.get("bitrateInfo")
    if isinstance(rates, list):
        for rate in rates:
            if not isinstance(rate, dict):
                continue
            address = rate.get("PlayAddr") or rate.get("playAddr")
            if isinstance(address, dict):
                variants = address.get("UrlList") or address.get("urlList")
                if isinstance(variants, list):
                    urls.extend(variants)
    urls.append(video.get("downloadAddr"))
    return list(
        dict.fromkeys(url for url in urls if isinstance(url, str) and url.startswith("https://"))
    )[:3]


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
            if self.on_media and not item.get("imagePost") and (urls := media_candidates(item)):
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
