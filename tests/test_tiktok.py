"""页面数据解析和选片规则，见爬取方案第 7 节。"""

from datetime import UTC, datetime

import pytest

from video_crawler import tiktok

CUTOFF = datetime(2026, 11, 1, tzinfo=UTC)
DAY = 86400


def item(work_id: int, days_before: float, pinned=False, photo=False) -> dict:
    """截止时间前 days_before 天发布的作品；负数表示截止时间之后。"""
    data = {
        "id": str(work_id),
        "createTime": int(CUTOFF.timestamp() - days_before * DAY),
        "author": {"uniqueId": "alice"},
        "desc": f"作品 {work_id}",
        "challenges": [{"title": "makeup"}],
    }
    if pinned:
        data["isPinnedItem"] = True
    if photo:
        data["imagePost"] = {"images": []}
    return data


def page(cursor: str, *items: dict, more=True) -> dict:
    return {"cursor": cursor, "hasMore": more, "itemList": list(items)}


def listing(limit=5) -> tiktok.Listing:
    return tiktok.Listing(CUTOFF, limit)


def test_settles_once_a_normal_work_is_older_than_the_fifth():
    works = listing()
    works.add("0", page("c1", *(item(100 - n, n) for n in range(1, 6))))
    assert not works.settled  # 第 5 名之后还没读到更早的普通作品
    works.add("c1", page("c2", item(90, 6)))
    assert works.settled
    assert [w["work_id"] for w in works.works()] == ["99", "98", "97", "96", "95"]


def test_pinned_works_rank_by_their_own_time_and_photos_keep_their_place():
    works = listing()
    works.add(
        "0",
        page(
            "c1",
            item(1, 400, pinned=True),  # 很早的置顶作品排不进前五
            item(2, 0.5, pinned=True),  # 新的置顶作品按时间排第一
            item(10, 1, photo=True),
            item(9, 2),
            item(8, 3),
            item(7, 4),
            item(6, 5),
        ),
    )
    result = works.works()
    assert [w["work_id"] for w in result] == ["2", "10", "9", "8", "7"]
    assert result[1]["type"] == "photo"
    assert result[1]["url"] == "https://www.tiktok.com/@alice/photo/10"
    assert result[0]["topics"] == ["makeup"]
    assert works.settled


def test_works_after_the_cutoff_are_left_out():
    works = listing(limit=2)
    works.add("0", page("c1", item(30, -2), item(20, 1), item(10, 2), item(5, 3)))
    assert [w["work_id"] for w in works.works()] == ["20", "10"]
    assert works.settled


def test_the_same_second_as_the_last_place_keeps_reading():
    works = listing(limit=2)
    works.add("0", page("c1", item(30, 1), item(20, 2), item(19, 2)))
    assert not works.settled  # 和第 2 名同一秒，可能还有 ID 更大的
    works.add("c1", page("c2", item(18, 3)))
    assert works.settled
    assert [w["work_id"] for w in works.works()] == ["30", "20"]


def test_the_last_page_settles_even_with_fewer_works():
    works = listing()
    works.add("0", page("c1", item(2, 1), item(1, 2), more=False))
    assert works.settled
    assert len(works.works()) == 2


def test_pages_that_do_not_follow_on_are_ignored():
    works = listing(limit=1)
    works.add("c9", page("c10", item(1, 1)))  # 接不上第一页
    assert not works.started
    works.add("0", page("c1", item(5, 1)))
    works.add("0", page("c1", item(5, 1)))  # 页面重载后又请求了一次第一页
    assert list(works.items) == ["5"]


def scope_with_user(**user) -> dict:
    return {"webapp.user-detail": {"statusCode": 0, "userInfo": {"user": user}}}


def test_profile_of_a_public_account():
    data = scope_with_user(id="680001", uniqueId="Alice", nickname="爱丽丝")
    assert tiktok.profile_of(data, "alice") == (
        None,
        {
            "handle": "Alice",
            "url": "https://www.tiktok.com/@Alice",
            "nickname": "爱丽丝",
            "platform_user_id": "680001",
        },
    )


def test_private_and_missing_accounts():
    private = scope_with_user(id="1", uniqueId="bob", privateAccount=True)
    assert tiktok.profile_of(private, "bob")[0] == "private"
    missing = {"webapp.user-detail": {"statusCode": 10221, "statusMsg": "user banned"}}
    assert tiktok.profile_of(missing, "nobody") == ("not_found", None)
    with pytest.raises(ValueError, match="不是 alice"):
        tiktok.profile_of(scope_with_user(id="1", uniqueId="someone"), "alice")


def video_scope(work_id="7", **video) -> dict:
    return {
        "webapp.video-detail": {
            "statusCode": 0,
            "itemInfo": {"itemStruct": {"id": work_id, "video": video}},
        }
    }


def test_media_candidates_in_order_without_duplicates():
    data = video_scope(
        playAddr="https://v16.tiktokcdn.com/play",
        bitrateInfo=[
            {
                "PlayAddr": {
                    "UrlList": ["https://v16.tiktokcdn.com/play", "https://v19.tiktokcdn.com/hd"]
                }
            }
        ],
        downloadAddr="https://v16.tiktokcdn.com/download",
    )
    assert tiktok.media_of(data, "7") == [
        "https://v16.tiktokcdn.com/play",
        "https://v19.tiktokcdn.com/hd",
        "https://v16.tiktokcdn.com/download",
    ]


def test_missing_or_other_videos():
    gone = {"webapp.video-detail": {"statusCode": 10204, "statusMsg": "item doesn't exist"}}
    with pytest.raises(LookupError):
        tiktok.media_of(gone, "7")
    with pytest.raises(ValueError):
        tiktok.media_of(video_scope(work_id="8", playAddr="https://x.tiktokcdn.com/a"), "7")


def test_media_cache_expires_is_bounded_and_preserves_a_newer_result(monkeypatch):
    now = 0
    monkeypatch.setattr(tiktok.time, "monotonic", lambda: now)
    cache = tiktok.MediaCache(ttl=480, limit=2)
    urls = ["https://v16.tiktokcdn.com/a"]
    cache.remember("identity-1", "old", urls)
    cache.remember("identity-1", "work", urls)
    previous = cache.get("work")
    now = 30
    cache.remember("identity-2", "work", urls)
    cache.discard("work", previous)
    cache.remember("identity-2", "another", urls)
    urls.append("https://v16.tiktokcdn.com/b")
    assert cache.get("old") is None
    assert cache.get("work").identity_id == "identity-2"
    assert cache.get("work").urls == tuple(urls[:1])
    now = 509
    assert cache.get("work") is not None
    now = 510
    assert cache.get("work") is None and cache.get("another") is None


def test_page_data_in_html_and_challenge_pages():
    html = '<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">'
    html += '{"__DEFAULT_SCOPE__": {"webapp.app-context": {}}}</script>'
    assert tiktok.scope_in_html(html) == {"webapp.app-context": {}}
    assert tiktok.scope_in_html("<html>Please wait...</html>") is None
