"""只把身份元数据存到 Redis，按设备隔离。"""

import json
from collections.abc import Collection

from redis import Redis

RETIRED_SINCE, RETIRED_SHOWN = 24 * 3600, 20


class IdentityStore:
    def __init__(self, client: Redis, instance: str) -> None:
        self._client = client
        self._active = f"video-crawler:{{{instance}}}:identities"
        self._retired = f"{self._active}:retired"

    def records(self) -> list[dict]:
        with self._client.pipeline(transaction=False) as pipe:
            active, retired = pipe.hvals(self._active).zrange(self._retired, 0, -1).execute()
        return [json.loads(record) for record in (*active, *retired)]

    def save(self, records: Collection[dict]) -> None:
        active, retired, retired_ids = {}, {}, []
        for item in records:
            body = json.dumps(item)
            if item["retired_at"] is None:
                active[item["id"]] = body
            else:
                retired[body] = item["retired_at"]
                retired_ids.append(item["id"])
        with self._client.pipeline() as pipe:
            if active:
                pipe.hset(self._active, mapping=active)
            if retired:
                pipe.hdel(self._active, *retired_ids)
                pipe.zadd(self._retired, retired)
                pipe.zremrangebyrank(self._retired, 0, -(RETIRED_SHOWN + 1))
            pipe.execute()
