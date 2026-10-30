import concurrent.futures
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from relayqueue import Queue, LeaseLost
from relayqueue.__main__ import run_once
from relayqueue.api import make_server


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "jobs.db"
        self.now = 1000.0
        self.q = Queue(self.path, clock=lambda: self.now)

    def test_restart_preserves_payload_and_result(self):
        jid = self.q.enqueue("echo", {"text": "Hello 🌍"})
        q2 = Queue(self.path, clock=lambda: self.now)
        job = q2.claim("one")
        q2.ack(jid, job["token"], job["payload"])
        self.assertEqual(Queue(self.path).get(jid)["result"], {"text": "Hello 🌍"})

    def test_concurrent_claims_never_share_job(self):
        ids = {self.q.enqueue("echo", i) for i in range(24)}
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            jobs = list(pool.map(lambda i: self.q.claim(str(i)), range(30)))
        claimed = [job["id"] for job in jobs if job]
        self.assertEqual(len(claimed), 24)
        self.assertEqual(set(claimed), ids)

    def test_enqueue_deduplicates_concurrently(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            ids = list(pool.map(lambda _: self.q.enqueue("echo", {"a": 1}, dedupe_key="same"), range(12)))
        self.assertEqual(len(set(ids)), 1)
        with self.assertRaises(ValueError):
            self.q.enqueue("echo", {"a": 2}, dedupe_key="same")

    def test_delay_boundary(self):
        self.q.enqueue("echo", 1, delay=5)
        self.assertIsNone(self.q.claim("one"))
        self.now += 5
        self.assertIsNotNone(self.q.claim("one"))

    def test_expired_owner_is_fenced(self):
        jid = self.q.enqueue("echo", 1)
        first = self.q.claim("old", lease_seconds=5)
        self.now += 5
        with self.assertRaises(LeaseLost):
            self.q.ack(jid, first["token"])
        second = self.q.claim("new")
        self.assertNotEqual(first["token"], second["token"])
        with self.assertRaises(LeaseLost):
            self.q.fail(jid, first["token"], "stale")
        self.q.ack(jid, second["token"], "new owner")

    def test_heartbeat_extends_lease(self):
        jid = self.q.enqueue("echo", None)
        job = self.q.claim("one", lease_seconds=5)
        self.now += 4
        self.q.heartbeat(jid, job["token"], lease_seconds=10)
        self.now += 5
        self.assertIsNone(self.q.claim("two"))
        self.q.ack(jid, job["token"])

    def test_expired_heartbeat_cannot_resurrect_lease(self):
        jid = self.q.enqueue("echo", None)
        job = self.q.claim("one", lease_seconds=1)
        self.now += 1
        with self.assertRaises(LeaseLost):
            self.q.heartbeat(jid, job["token"])

    def test_backoff_dead_letter_and_manual_retry(self):
        jid = self.q.enqueue("bad", {}, max_attempts=2)
        first = self.q.claim("one")
        self.q.fail(jid, first["token"], "boom", retry_base=2)
        self.assertIsNone(self.q.claim("two"))
        self.now += 2
        second = self.q.claim("two")
        self.q.fail(jid, second["token"], "again")
        self.assertEqual(self.q.get(jid)["state"], "dead")
        self.assertIsNone(self.q.claim("three"))
        self.q.retry(jid)
        self.assertEqual(self.q.claim("four")["attempts"], 1)

    def test_crashes_exhaust_attempts(self):
        jid = self.q.enqueue("echo", 1, max_attempts=1)
        self.q.claim("crashed", lease_seconds=1)
        self.now += 1
        self.assertEqual(self.q.recover(), 1)
        self.assertEqual(self.q.get(jid)["state"], "dead")
        self.assertIsNone(self.q.claim("another"))

    def test_terminal_job_cannot_be_acked_twice(self):
        jid = self.q.enqueue("echo", 1)
        job = self.q.claim("one")
        self.q.ack(jid, job["token"], 10)
        with self.assertRaises(LeaseLost):
            self.q.ack(jid, job["token"], 20)
        self.assertEqual(self.q.get(jid)["result"], 10)

    def test_invalid_inputs_do_not_create_jobs(self):
        for options in ({"kind": ""}, {"max_attempts": True}, {"delay": float("nan")}, {"max_attempts": 0}, {"dedupe_key": ""}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.q.enqueue(**({"kind": "echo", "payload": 1} | options))
        with self.assertRaises(ValueError):
            self.q.enqueue("echo", float("nan"))
        self.assertEqual(self.q.jobs(), [])

    def test_inspection_filters_and_validates(self):
        self.q.enqueue("echo", 1)
        self.q.enqueue("echo", 2)
        self.assertEqual(len(self.q.jobs(limit=1, offset=1)), 1)
        self.assertEqual(self.q.jobs(state="dead"), [])
        with self.assertRaises(ValueError):
            self.q.jobs(state="oops")
        with self.assertRaises(ValueError):
            self.q.jobs(limit=101)

    def test_event_history_survives_restart(self):
        jid = self.q.enqueue("echo", 1)
        job = self.q.claim("one")
        self.q.ack(jid, job["token"])
        self.assertEqual([e["event"] for e in Queue(self.path).history(jid)], ["enqueued", "claimed", "completed"])

    def test_worker_dispatches_and_handles_errors(self):
        good = self.q.enqueue("double", 5)
        run_once(self.q, {"double": lambda x: x * 2})
        self.assertEqual(self.q.get(good)["result"], 10)
        bad = self.q.enqueue("unknown", 1, max_attempts=1)
        run_once(self.q, {})
        self.assertEqual(self.q.get(bad)["state"], "dead")
        self.assertFalse(run_once(self.q, {}))


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.q = Queue(Path(self.tmp.name) / "api.db")
        self.server = make_server(self.q, port=0, token="test-only-token")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.close_server)

    def close_server(self):
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()

    def request(self, path, data=None, authorized=True):
        headers = {"Authorization": "Bearer test-only-token"} if authorized else {}
        request = urllib.request.Request(f"http://127.0.0.1:{self.server.server_port}{path}",
                                         data=None if data is None else json.dumps(data).encode(), headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_auth_and_real_http_flow(self):
        self.assertEqual(self.request("/health", authorized=False)[0], 401)
        self.assertEqual(self.request("/health")[0], 200)
        status, body = self.request("/jobs", {"kind": "echo", "payload": {"hello": "world"}})
        self.assertEqual(status, 201)
        job = self.q.claim("one")
        code, detail = self.request("/jobs/" + body["id"])
        self.assertEqual(code, 200)
        self.assertEqual(detail["job"]["state"], "running")
        self.assertNotIn("token", detail["job"])
        self.assertNotIn("token", self.request("/jobs")[1]["jobs"][0])
        self.q.ack(body["id"], job["token"], "ok")
        self.assertEqual(self.request("/jobs/" + body["id"])[1]["job"]["result"], "ok")

    def test_invalid_requests_and_missing_jobs(self):
        self.assertEqual(self.request("/jobs/missing")[0], 404)
        self.assertEqual(self.request("/jobs?limit=no")[0], 400)
        self.assertEqual(self.request("/jobs", {"kind": "echo", "unexpected": 1})[0], 400)
        self.assertEqual(self.request("/jobs", {"kind": "echo", "payload": "x" * 66000})[0], 413)


if __name__ == "__main__":
    unittest.main()
