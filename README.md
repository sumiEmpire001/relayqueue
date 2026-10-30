# RelayQueue

A durable Python job queue for one machine: transactional claims, fenced worker leases, automatic lease renewal, exponential retries, dead letters, a CLI and an authenticated inspection API.

Built for Lord_Sumi's portfolio. This is an independently implemented educational service with a working core; it is not a hosted queue or a claim of production certification.

## Try it

Python 3.10+ is required. Runtime dependencies: Python standard library only.

```bash
git clone https://github.com/sumiEmpire001/relayqueue.git
cd relayqueue
python -m relayqueue --db demo.db enqueue sum '{"numbers":[10,20,30]}' --key demo-sum
python -m relayqueue --db demo.db worker --once
python -m relayqueue --db demo.db list --state done
python -m unittest discover -s tests -v
```

The completed job contains the result `60`. Run commands from the repository root, or install with `python -m pip install -e .` to use the `relayqueue` command.

Other commands:

```bash
python -m relayqueue --db demo.db show JOB_ID
python -m relayqueue --db demo.db recover
python -m relayqueue --db demo.db retry DEAD_JOB_ID
python -m relayqueue --db demo.db worker
```

`worker` handles `echo` and `sum`; unknown kinds fail and eventually enter the dead-letter state. Ctrl+C or SIGTERM stops polling after the current handler finishes. Error text and payloads are stored in the database; submit only data you intend to retain locally.

## API

Set `RELAYQUEUE_API_TOKEN` in your terminal to a random local secret, then start:

```bash
python -m relayqueue --db demo.db serve --port 8080
```

Every request requires `Authorization: Bearer YOUR_LOCAL_TOKEN`.

| Method | Route | Behavior |
|---|---|---|
| GET | `/health` | Check server response |
| POST | `/jobs` | Submit `{ "kind": "echo", "payload": {"message":"hello"} }` |
| GET | `/jobs?state=done&limit=20&offset=0` | Paginated job inspection |
| GET | `/jobs/JOB_ID` | Job details and ordered event history |

Optional submission fields: `delay`, `max_attempts`, `dedupe_key`. Request bodies are capped at 64 KiB. Lease tokens are excluded from inspection responses. The service binds to **127.0.0.1 only**. It uses Python's development HTTP server and has no TLS, multi-user authorization, network rate limits or tenant separation. Keep it local.

## Use your own handler

```python
from relayqueue import Queue
from relayqueue.__main__ import run_once

queue = Queue("tasks.db")
job_id = queue.enqueue("uppercase", {"text": "hello"}, dedupe_key="demo-uppercase")
run_once(queue, {"uppercase": lambda p: p["text"].upper()}, worker="demo-worker")
assert queue.get(job_id)["result"] == "HELLO"
```

Handlers receive JSON payloads and return JSON values. A renewal thread extends the lease while the handler executes. Handler errors are recorded and retried. A lost renewal prevents committing a result, but cannot undo application side effects or interrupt arbitrary Python code. Design those effects to be idempotent.

## Architecture and guarantees

- Each operation opens its own SQLite connection. Writes use `BEGIN IMMEDIATE` so concurrent claimers cannot select the same available job.
- WAL mode supports inspection while writes proceed. Jobs and their audit events commit together.
- Claims receive random tokens and deadlines. Completion, failure and heartbeat require a current token and an unexpired lease.
- Claiming recovers abandoned leases. Attempts increment on claim; exhausted attempts become dead letters.
- Retry delay doubles after each failure and is capped at one hour. Manual retry explicitly resets the attempt budget.
- An enqueue key deduplicates the same kind, canonical JSON payload and attempt budget. Reusing a key for different content raises an error. A repeated request's delay does not reschedule the original job.
- Execution is **at least once**, not exactly once. A crash after an external side effect and before acknowledgment can cause that effect to run again.

See [verification](docs/verification.md) for evidence and [design](docs/design.md) for boundaries and failure cases.

## Limitations and next work

Single-host storage; no network-mounted SQLite, distributed consensus, priorities, cron scheduling, encrypted storage, schema migration tooling or retention cleanup. The library API has no payload byte limit; the HTTP API does. Queue ordering is by available time, creation time and ID. Workers share the same database file. Wall-clock changes can affect lease deadlines. Back up the database with SQLite-aware tooling. CI is configured; its remote result must be checked separately from local tests.

## License

MIT — see [LICENSE](LICENSE).
