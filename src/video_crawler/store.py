"""只把身份元数据存到 Redis，按设备隔离。"""

import json
from collections.abc import Collection

from redis import Redis

RETIRED_SINCE, RETIRED_SHOWN = 24 * 3600, 20


class IdentityStore:
    def __init__(self, client: Redis, instance: str, *, keep_profile_dirs: bool = False) -> None:
        self._client = client
        self._active = f"video-crawler:{{{instance}}}:identities"
        self._retired = f"{self._active}:retired"
        self._kept_retired = f"{self._active}:kept-retired"
        self._keep_profile_dirs = keep_profile_dirs

    def records(self) -> list[dict]:
        with self._client.pipeline(transaction=False) as pipe:
            pipe.hvals(self._active).zrange(self._retired, 0, -1)
            if self._keep_profile_dirs:
                pipe.hvals(self._kept_retired)
            active, recent, *kept = pipe.execute()
        records = [json.loads(record) for record in active]
        if not self._keep_profile_dirs:
            return records + [json.loads(record) for record in recent]
        archived = [json.loads(body) for body in kept[0]]
        retired = {entry["id"]: entry for entry in archived}
        # 接管已存在的退休记录，防止最近记录滚动后把保留下来的目录当成无主资料。
        legacy = {}
        for body in reversed(recent):
            entry = json.loads(body)
            if entry["id"] not in retired:
                legacy[entry["id"]] = body
                retired[entry["id"]] = entry
        if legacy:
            self._client.hset(self._kept_retired, mapping=legacy)
        return records + sorted(retired.values(), key=lambda record: record["retired_at"])

    def save(self, records: Collection[dict]) -> None:
        active, retired, retired_by_id = {}, {}, {}
        for item in records:
            body = json.dumps(item)
            if item["retired_at"] is None:
                active[item["id"]] = body
            else:
                retired[body] = item["retired_at"]
                retired_by_id[item["id"]] = body
        with self._client.pipeline() as pipe:
            if active:
                pipe.hset(self._active, mapping=active)
            if retired:
                pipe.hdel(self._active, *retired_by_id)
                if self._keep_profile_dirs:
                    pipe.hset(self._kept_retired, mapping=retired_by_id)
                pipe.zadd(self._retired, retired)
                pipe.zremrangebyrank(self._retired, 0, -(RETIRED_SHOWN + 1))
            pipe.execute()
