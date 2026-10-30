# sqlite-task-queue 📬

A durable, distributed-style task queue built on **SQLite — stdlib only**.
The semantics that are usually hard are done correctly and covered by tests.

## Test results

```
[PASS] concurrency: 4 workers processed 50 tasks, zero double-claims
[PASS] crash recovery: task claimed by dead worker was re-claimed after lease expiry
[PASS] failure handling: ['retry-in-2s', 'retry-in-4s', 'dead-letter'] -> dead-letter queue holds 1 task
[PASS] priority & delay: high-priority first, delayed task invisible until due
```

## Semantics

| Concern | How it's handled |
|---|---|
| Exactly-once claiming | Atomic `UPDATE ... RETURNING` under WAL |
| Worker crashes | Leases with expiry; expired claims are auto-reclaimed |
| Failing tasks | `nack()` → exponential backoff (2s/4s/8s…) → dead-letter after max attempts |
| Scheduling | `priority` (higher first) + `delay_seconds` (runs later) |
| Heartbeats | Long tasks extend their lease with `heartbeat()` |

## Usage

```python
from task_queue import TaskQueue
q = TaskQueue("jobs.db")
q.enqueue("emails", {"to": "bob"}, priority=5, delay_seconds=10)

task = q.claim("emails", worker="w1", lease_seconds=60)
# ... work ...
q.ack(task)                    # success
q.nack(task, error="...")      # or fail -> backoff / dead-letter
```

Run the full test suite: `python task_queue.py demo`

## License

MIT
