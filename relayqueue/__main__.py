import argparse
import json
import signal
import threading
import uuid

from .queue import Queue, LeaseLost


def run_once(queue, handlers, worker="worker", lease_seconds=30):
    job = queue.claim(worker, lease_seconds=lease_seconds)
    if job is None:
        return False
    stopped = threading.Event()
    lost = []

    def renew():
        while not stopped.wait(lease_seconds / 3):
            try:
                queue.heartbeat(job["id"], job["token"], lease_seconds=lease_seconds)
            except Exception as exc:
                lost.append(exc)
                return

    thread = threading.Thread(target=renew, daemon=True)
    thread.start()
    result, failure = None, None
    try:
        result = handlers[job["kind"]](job["payload"])
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
    finally:
        stopped.set()
        thread.join()
    if lost:
        raise LeaseLost("Heartbeat failed; refusing to commit handler result") from lost[0]
    if failure is None:
        queue.ack(job["id"], job["token"], result)
    else:
        queue.fail(job["id"], job["token"], failure)
    return True


def main():
    parser = argparse.ArgumentParser(description="Durable local job queue")
    parser.add_argument("--db", default="queue.db")
    subs = parser.add_subparsers(dest="command", required=True)
    put = subs.add_parser("enqueue")
    put.add_argument("kind")
    put.add_argument("payload", help="JSON value")
    put.add_argument("--key")
    put.add_argument("--max-attempts", type=int, default=3)
    ls = subs.add_parser("list")
    ls.add_argument("--state", choices=["queued", "running", "done", "dead"])
    ls.add_argument("--offset", type=int, default=0)
    show = subs.add_parser("show")
    show.add_argument("id")
    retry = subs.add_parser("retry")
    retry.add_argument("id")
    subs.add_parser("recover")
    worker = subs.add_parser("worker")
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--lease", type=float, default=30)
    serve = subs.add_parser("serve")
    serve.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    queue = Queue(args.db)
    try:
        if args.command == "enqueue":
            print(queue.enqueue(args.kind, json.loads(args.payload), dedupe_key=args.key, max_attempts=args.max_attempts))
        elif args.command == "list":
            jobs = queue.jobs(state=args.state, offset=args.offset)
            for job in jobs:
                job.pop("token", None)
            print(json.dumps(jobs, indent=2))
        elif args.command == "show":
            job = queue.get(args.id)
            if not job:
                parser.error("Job not found")
            job.pop("token", None)
            print(json.dumps({"job": job, "history": queue.history(args.id)}, indent=2))
        elif args.command == "retry":
            queue.retry(args.id)
        elif args.command == "recover":
            print(queue.recover())
        elif args.command == "serve":
            from .api import serve
            serve(queue, args.port)
        elif args.command == "worker":
            stop = threading.Event()
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, lambda *_: stop.set())
            handlers = {"echo": lambda payload: payload, "sum": lambda payload: sum(payload["numbers"])}
            while not stop.is_set():
                try:
                    worked = run_once(queue, handlers, uuid.uuid4().hex, args.lease)
                except LeaseLost as exc:
                    print(str(exc))
                    worked = False
                if args.once:
                    break
                if not worked:
                    stop.wait(0.5)
    except (ValueError, TypeError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
