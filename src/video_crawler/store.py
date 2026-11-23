"""本地状态表（SQLite）：任务、待发事件、批次的暂停和取消、身份。程序重启后接着用。

收任务的 Dramatiq 线程和主事件循环共用一个连接，写入都在锁里做。
"""

import json
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    run INTEGER NOT NULL,
    kind TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    body TEXT NOT NULL,
    received_at REAL NOT NULL,
    done INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_at REAL NOT NULL,
    avoid TEXT NOT NULL DEFAULT '[]',
    history TEXT NOT NULL DEFAULT '[]',
    waiting TEXT
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    body TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS identities (
    id TEXT PRIMARY KEY,
    fails INTEGER NOT NULL DEFAULT 0,
    cooldown_until REAL NOT NULL DEFAULT 0,
    last_used_at REAL NOT NULL DEFAULT 0,
    warmed_at REAL,
    browser TEXT,
    retired_at REAL,
    retire_reason TEXT
);
"""


@dataclass
class Task:
    task_id: str
    run: int
    kind: str
    """crawl_creator 或 download_video。"""
    batch_id: str
    body: dict
    """调度中心发来的任务原文。"""
    attempts: int = 0
    next_at: float = 0
    """什么时候可以开始下一次尝试。"""
    avoid: list[str] = field(default_factory=list)
    """这个任务失败过的线路，申请线路时避开。"""
    history: list[str] = field(default_factory=list)
    """每次没做成的原因，最后失败时一起回传。"""
    waiting: str | None = None
    """在等什么：线路、身份、磁盘空间……只用于显示。"""


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(SCHEMA)
        self._lock = threading.Lock()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._db.execute("COMMIT")

    def _read(self, sql: str, *params) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, params).fetchall()

    # ---- 任务 ----

    def receive(self, kind: str, body: dict, accepted: dict) -> bool:
        """登记收到的任务并排好 accepted 事件；重复、旧轮次或已取消批次的任务返回 False。"""
        with self._write() as db:
            if db.execute(
                "SELECT 1 FROM batches WHERE batch_id = ? AND state = 'cancelled'",
                (body["batch_id"],),
            ).fetchone():
                return False
            row = db.execute(
                "SELECT run FROM tasks WHERE task_id = ?", (body["task_id"],)
            ).fetchone()
            if row and row["run"] >= body["run"]:
                return False
            now = time.time()
            db.execute(
                "INSERT OR REPLACE INTO tasks"
                " (task_id, run, kind, batch_id, body, received_at, next_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (body["task_id"], body["run"], kind, body["batch_id"], json.dumps(body), now, now),
            )
            _queue_event(db, accepted)
            return True

    def due(self, kind: str, now: float, running: set[str]) -> Task | None:
        """按收到的先后，取一个到了时间的任务。批次暂停采集时不开始新的获取作品，下载照常。"""
        rows = self._read(
            "SELECT * FROM tasks WHERE kind = ? AND done = 0 AND next_at <= ?"
            " AND (kind != 'crawl_creator' OR batch_id NOT IN (SELECT batch_id FROM batches))"
            " ORDER BY received_at",
            kind,
            now,
        )
        row = next((row for row in rows if row["task_id"] not in running), None)
        return row and _task(row)

    def save(self, task: Task) -> None:
        with self._write() as db:
            db.execute(
                "UPDATE tasks SET attempts = ?, next_at = ?, avoid = ?, history = ?, waiting = ?"
                " WHERE task_id = ? AND run = ?",
                (
                    task.attempts,
                    task.next_at,
                    json.dumps(task.avoid),
                    json.dumps(task.history, ensure_ascii=False),
                    task.waiting,
                    task.task_id,
                    task.run,
                ),
            )

    def finish(self, task: Task, event: dict | None) -> None:
        """任务结束，同时排好要回传的事件；这一轮已被取消或取代时不再回传。"""
        with self._write() as db:
            changed = db.execute(
                "UPDATE tasks SET done = 1, waiting = NULL"
                " WHERE task_id = ? AND run = ? AND done = 0",
                (task.task_id, task.run),
            ).rowcount
            if changed and event:
                _queue_event(db, event)

    def current(self, task: Task) -> bool:
        """这一轮还要不要做：没结束、没被取代、批次没取消。"""
        return bool(
            self._read(
                "SELECT 1 FROM tasks WHERE task_id = ? AND run = ? AND done = 0 AND batch_id NOT IN"
                " (SELECT batch_id FROM batches WHERE state = 'cancelled')",
                task.task_id,
                task.run,
            )
        )

    def unfinished(self) -> dict[str, int]:
        """每类还没做完的任务数，包括正在做的。"""
        rows = self._read("SELECT kind, count(*) AS n FROM tasks WHERE done = 0 GROUP BY kind")
        return {row["kind"]: row["n"] for row in rows}

    # ---- 批次暂停和取消 ----

    def pause(self, batch_id: str) -> None:
        with self._write() as db:
            db.execute(
                "INSERT OR IGNORE INTO batches (batch_id, state) VALUES (?, 'paused')", (batch_id,)
            )

    def resume(self, batch_id: str) -> None:
        with self._write() as db:
            db.execute("DELETE FROM batches WHERE batch_id = ? AND state = 'paused'", (batch_id,))

    def cancel_batch(self, batch_id: str) -> None:
        with self._write() as db:
            db.execute(
                "INSERT OR REPLACE INTO batches (batch_id, state) VALUES (?, 'cancelled')",
                (batch_id,),
            )
            db.execute("UPDATE tasks SET done = 1 WHERE batch_id = ?", (batch_id,))

    def cancel_task(self, task_id: str) -> None:
        with self._write() as db:
            db.execute("UPDATE tasks SET done = 1 WHERE task_id = ?", (task_id,))

    def paused(self) -> list[str]:
        return [
            row["batch_id"]
            for row in self._read("SELECT batch_id FROM batches WHERE state = 'paused'")
        ]

    # ---- 待发事件 ----

    def events(self) -> list[dict]:
        return [
            json.loads(row["body"])
            for row in self._read("SELECT body FROM outbox ORDER BY created_at")
        ]

    def sent(self, event_id: str) -> None:
        with self._write() as db:
            db.execute("DELETE FROM outbox WHERE event_id = ?", (event_id,))

    # ---- 身份 ----

    def identities(self) -> list[dict]:
        return [
            dict(row) for row in self._read("SELECT * FROM identities WHERE retired_at IS NULL")
        ]

    def retired(self, since: float, limit: int) -> list[dict]:
        """最近退休的身份，新的在前。"""
        sql = "SELECT * FROM identities WHERE retired_at >= ? ORDER BY retired_at DESC LIMIT ?"
        return [dict(row) for row in self._read(sql, since, limit)]

    def save_identity(self, record: dict) -> None:
        columns, marks = ", ".join(record), ", ".join("?" * len(record))
        with self._write() as db:
            db.execute(
                f"INSERT OR REPLACE INTO identities ({columns}) VALUES ({marks})",
                tuple(record.values()),
            )


def _queue_event(db: sqlite3.Connection, event: dict) -> None:
    db.execute(
        "INSERT OR IGNORE INTO outbox (event_id, body, created_at) VALUES (?, ?, ?)",
        (event["event_id"], json.dumps(event), time.time()),
    )


def _task(row: sqlite3.Row) -> Task:
    return Task(
        task_id=row["task_id"],
        run=row["run"],
        kind=row["kind"],
        batch_id=row["batch_id"],
        body=json.loads(row["body"]),
        attempts=row["attempts"],
        next_at=row["next_at"],
        avoid=json.loads(row["avoid"]),
        history=json.loads(row["history"]),
        waiting=row["waiting"],
    )
