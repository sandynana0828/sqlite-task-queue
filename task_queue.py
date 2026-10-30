#!/usr/bin/env python3
"""task_queue.py - a durable, distributed-style task queue on SQLite (stdlib only).

Semantics (the hard parts done right):
  * **Atomic claim** — `UPDATE ... RETURNING` with a lease so two workers can never
    grab the same task, even across processes.
  * **Leases & heartbeats** — a claimed task has a deadline; if a worker crashes,
    the task automatically becomes claimable again after the lease expires.
  * **Retry with exponential backoff** — `nack()` schedules the task for a later
    retry; after `max_attempts` it moves to the **dead-letter queue**.
  * **Priority + delay** — enqueue with priority (higher first) and `delay_seconds`
    for scheduled execution.
  * WAL journal mode for concurrent readers/writers.

Usage:
    from task_queue import TaskQueue
    q = TaskQueue("jobs.db")
    q.enqueue("emails", {"to": "bob"}, priority=5, delay_seconds=10)

    task = q.claim("emails", worker="w1", lease_seconds=60)
    ... do work ...
    q.ack(task)          # success
    q.nack(task)         # or: retry later / dead-letter after max_attempts

Run `python task_queue.py demo` for a concurrency + crash-recovery demo.
"""
from __future__ import annotations

import json, sqlite3, time


class TaskQueue:
    def __init__(self, path: str = "tasks.db", max_attempts: int = 3):
        self.conn = sqlite3.connect(path, isolation_level=None)  # autocommit
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.max_attempts = max_attempts
        self._migrate()

    def _migrate(self):
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS tasks (
            id          INTEGER PRIMARY KEY,
            queue       TEXT NOT NULL,
            payload     TEXT NOT NULL,
            priority    INTEGER NOT NULL DEFAULT 0,
            status      TEXT NOT NULL DEFAULT 'ready',  -- ready|delayed|claimed|done|dead
            attempts    INTEGER NOT NULL DEFAULT 0,
            run_at      REAL NOT NULL DEFAULT 0,        -- unix ts when claimable
            lease_until REAL,
            worker      TEXT,
            claimed_at  REAL,
            done_at     REAL,
            last_error  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_claim ON tasks(queue, status, run_at, priority DESC);
        CREATE TABLE IF NOT EXISTS dead (
            id INTEGER PRIMARY KEY, queue TEXT, payload TEXT, attempts INTEGER,
            last_error TEXT, died_at REAL
        );
        """)

    # ---------- producers ----------
    def enqueue(self, queue: str, payload: dict, priority: int = 0,
                delay_seconds: float = 0) -> int:
        cur = self.conn.execute(
            "INSERT INTO tasks(queue,payload,priority,status,run_at) VALUES(?,?,?,?,?)",
            (queue, json.dumps(payload), priority,
             "delayed" if delay_seconds > 0 else "ready",
             time.time() + delay_seconds))
        return cur.lastrowid

    # ---------- consumers ----------
    def claim(self, queue: str, worker: str, lease_seconds: float = 60) -> dict | None:
        """Atomically claim the highest-priority runnable task, or None."""
        now = time.time()
        self.conn.execute(
            "UPDATE tasks SET status='ready' WHERE queue=? AND status='delayed' AND run_at<=?",
            (queue, now))
        # reclaim expired leases (crashed workers)
        self.conn.execute(
            "UPDATE tasks SET status='ready', worker=NULL, lease_until=NULL "
            "WHERE queue=? AND status='claimed' AND lease_until<=?", (queue, now))
        row = self.conn.execute(
            """UPDATE tasks SET status='claimed', worker=?, claimed_at=?, lease_until=?
               WHERE id = (
                 SELECT id FROM tasks WHERE queue=? AND status='ready' AND run_at<=?
                 ORDER BY priority DESC, id ASC LIMIT 1
               )
               RETURNING id, payload, attempts""",
            (worker, now, now + lease_seconds, queue, now)).fetchone()
        if not row:
            return None
        return {"id": row[0], "payload": json.loads(row[1]), "attempts": row[2],
                "worker": worker}

    def heartbeat(self, task_id: int, lease_seconds: float = 60) -> bool:
        cur = self.conn.execute(
            "UPDATE tasks SET lease_until=? WHERE id=? AND status='claimed'",
            (time.time() + lease_seconds, task_id))
        return cur.rowcount > 0

    def ack(self, task_id: int) -> bool:
        cur = self.conn.execute(
            "UPDATE tasks SET status='done', done_at=? WHERE id=? AND status='claimed'",
            (time.time(), task_id))
        return cur.rowcount > 0

    def nack(self, task_id: int, error: str = "", backoff_base: float = 2.0) -> str:
        """Mark failure: retry with exponential backoff, or dead-letter."""
        row = self.conn.execute(
            "SELECT attempts, payload, queue FROM tasks WHERE id=? AND status='claimed'",
            (task_id,)).fetchone()
        if not row:
            return "not-claimed"
        attempts, payload, queue = row
        attempts += 1
        if attempts >= self.max_attempts:
            self.conn.execute("BEGIN")
            self.conn.execute("INSERT INTO dead(id,queue,payload,attempts,last_error,died_at) "
                              "VALUES(?,?,?,?,?,?)", (task_id, queue, payload, attempts,
                                                      error, time.time()))
            self.conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
            self.conn.execute("COMMIT")
            return "dead-letter"
        delay = backoff_base ** attempts  # 2s, 4s, 8s ...
        self.conn.execute(
            "UPDATE tasks SET status='delayed', run_at=?, attempts=?, last_error=?, "
            "worker=NULL, lease_until=NULL WHERE id=?",
            (time.time() + delay, attempts, error, task_id))
        return f"retry-in-{delay:.0f}s"

    def stats(self) -> dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) FROM tasks GROUP BY status").fetchall()
        dead = self.conn.execute("SELECT COUNT(*) FROM dead").fetchone()[0]
        return dict(rows) | {"dead": dead}

    def purge_done(self):
        self.conn.execute("DELETE FROM tasks WHERE status='done'")


# ---------------- demo & concurrency test ----------------
def demo():
    import os, threading
    db = "demo_tasks.db"
    for f in (db, db + "-wal", db + "-shm"):
        if os.path.exists(f):
            os.remove(f)

    q = TaskQueue(db)
    for i in range(50):
        q.enqueue("jobs", {"n": i, "op": "square"}, priority=i % 3)

    # --- test 1: 4 threads race to claim; every task claimed exactly once ---
    processed, lock = [], threading.Lock()

    def worker(name):
        wq = TaskQueue(db)
        while True:
            t = wq.claim("jobs", worker=name, lease_seconds=30)
            if t is None:
                return
            time.sleep(0.005)  # simulate work
            assert t["payload"]["op"] == "square"
            wq.ack(t["id"])
            with lock:
                processed.append(t["id"])

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(processed) == 50 and len(set(processed)) == 50, "double-claim detected!"
    print(f"[PASS] concurrency: 4 workers processed 50 tasks, zero double-claims")

    # --- test 2: crash recovery — claim without ack, lease expires, task returns ---
    q.enqueue("jobs", {"n": 999, "op": "square"})
    t = q.claim("jobs", worker="ghost-worker", lease_seconds=0.5)
    assert t is not None
    time.sleep(0.7)  # ghost worker 'crashes' (never acks), lease expires
    t2 = q.claim("jobs", worker="recovery-worker", lease_seconds=30)
    assert t2 is not None and t2["id"] == t["id"], "crashed task not recovered!"
    q.ack(t2["id"])
    print(f"[PASS] crash recovery: task claimed by dead worker was re-claimed after lease expiry")

    # --- test 3: retries with backoff then dead-letter ---
    q.enqueue("jobs", {"n": -1, "op": "boom"}, priority=9)
    outcomes = []
    while True:
        t = q.claim("jobs", worker="w-fail", lease_seconds=30)
        if t is None:
            break
        outcomes.append(q.nack(t["id"], error="boom"))
        if outcomes[-1].startswith("retry"):
            # fast-forward the backoff by patching run_at
            q.conn.execute("UPDATE tasks SET run_at=0 WHERE payload=?", (json.dumps({"n": -1, "op": "boom"}),))
    assert outcomes == ["retry-in-2s", "retry-in-4s", "dead-letter"], outcomes
    st = q.stats()
    assert st.get("dead") == 1
    print(f"[PASS] failure handling: {outcomes} -> dead-letter queue holds {st['dead']} task")

    # --- test 4: priority + delay ---
    q.enqueue("jobs", {"n": 1, "op": "low"}, priority=1)
    q.enqueue("jobs", {"n": 2, "op": "high"}, priority=9)
    q.enqueue("jobs", {"n": 3, "op": "delayed"}, priority=9, delay_seconds=60)
    t = q.claim("jobs", worker="p", lease_seconds=30)
    assert t["payload"]["op"] == "high", "priority order wrong!"
    q.ack(t["id"])
    t = q.claim("jobs", worker="p", lease_seconds=30)
    assert t["payload"]["op"] == "low", "delayed task was claimed too early!"
    q.ack(t["id"])
    t = q.claim("jobs", worker="p", lease_seconds=30)
    assert t is None, "delayed task should not be claimable yet!"
    print("[PASS] priority & delay: high-priority first, delayed task invisible until due")

    print(f"\nfinal stats: {q.stats()}")
    print("all task-queue tests passed")


if __name__ == "__main__":
    demo()
