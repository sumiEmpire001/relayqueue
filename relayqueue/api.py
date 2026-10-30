"""Authenticated loopback inspection API. Not an Internet-facing server."""
import hmac
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


def make_server(queue, port=8080, token=None):
    token = token or os.environ.get("RELAYQUEUE_API_TOKEN")
    if not token:
        raise ValueError("Set RELAYQUEUE_API_TOKEN before starting the API")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass  # Avoid logging payloads or authorization headers.

        def reply(self, status, body):
            data = json.dumps(body, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self.dispatch()

        def do_POST(self):
            self.dispatch()

        def dispatch(self):
            supplied = self.headers.get("Authorization", "").encode()
            if not hmac.compare_digest(supplied, ("Bearer " + token).encode()):
                self.reply(401, {"error": "Unauthorized"})
                return
            try:
                parts = urlsplit(self.path)
                path = parts.path
                if self.command == "GET" and path == "/health":
                    self.reply(200, {"status": "ok"})
                elif self.command == "GET" and path == "/jobs":
                    query = parse_qs(parts.query)
                    jobs = queue.jobs(state=query.get("state", [None])[0], limit=int(query.get("limit", [50])[0]), offset=int(query.get("offset", [0])[0]))
                    for job in jobs:
                        job.pop("token", None)
                    self.reply(200, {"jobs": jobs})
                elif self.command == "GET" and path.startswith("/jobs/"):
                    job = queue.get(path[len("/jobs/"):])
                    if job is None:
                        self.reply(404, {"error": "Job not found"})
                    else:
                        job.pop("token", None)
                        self.reply(200, {"job": job, "history": queue.history(job["id"])})
                elif self.command == "POST" and path == "/jobs":
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 < length <= 65536 or self.headers.get("Transfer-Encoding"):
                        self.reply(413, {"error": "A JSON body of 1..65536 bytes with Content-Length is required"})
                        return
                    self.connection.settimeout(5)
                    value = json.loads(self.rfile.read(length))
                    if not isinstance(value, dict) or set(value) - {"kind", "payload", "max_attempts", "delay", "dedupe_key"}:
                        raise ValueError("Expected a job object with known fields")
                    job_id = queue.enqueue(**value)
                    self.reply(201, {"id": job_id})
                else:
                    self.reply(404, {"error": "Route not found"})
            except (ValueError, TypeError, UnicodeError) as exc:
                self.reply(400, {"error": str(exc)})
            except TimeoutError:
                self.reply(408, {"error": "Request body timed out"})
            except Exception:
                self.reply(500, {"error": "Internal error"})

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def serve(queue, port):
    server = make_server(queue, port)
    print(f"RelayQueue listening on http://127.0.0.1:{server.server_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
