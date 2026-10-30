# Verification

Local validation on 30 October 2026: `python -m unittest discover -s tests -v` passed **16 tests**.

Coverage includes 30 concurrent claim calls against 24 jobs, concurrent idempotent submissions, reopen persistence, delayed eligibility boundaries, stale-worker fencing, heartbeat renewal, terminal state protection, abandoned lease recovery, exhausted retry budgets, manual replay, ordered audit history, invalid inputs and worker error handling.

API checks start a real local HTTP server on an ephemeral port and send authenticated/unauthenticated requests, submit a job, claim and complete it, inspect the result, reject malformed requests, enforce request size and verify lease tokens are absent from inspection responses.

These are thread concurrency and process-reopen tests. They do not establish behavior under disk corruption, host power loss, filesystem faults, multiprocess stress, long-term load or production network exposure. GitHub Actions is configured separately for Python 3.10, 3.12 and 3.13; a workflow file alone does not establish remote CI success.
