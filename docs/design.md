# State and failure model

`queued → running → done` is the successful path. Handler failure returns a job to `queued` with backoff, or moves it to `dead` when its attempt budget is exhausted. An expired lease follows the same budget decision during recovery. An explicit dead-letter retry returns it to `queued` and resets attempts.

## Transactions

Claim, enqueue, fail, complete and retry are serialized SQLite writes. Selecting a job and changing its state happen in the same write transaction. Each connection has a ten-second SQLite lock wait. Errors roll back the operation and its event together.

## Fencing

A unique claim token is required for heartbeat, acknowledgment and failure. Deadline checks prevent a delayed worker from committing even before a replacement worker claims the abandoned job. The token fences database state, not external actions. External systems need an idempotency key or their own fencing mechanism.

## Recovery

The next claim reclaims expired work, or an operator runs `recover`. No scheduler daemon is required. If no worker or operator runs, expired rows remain visible as running until recovery. The worker renews active leases in a thread and drains its current handler on shutdown. There is no forced shutdown deadline or cancellation of a stuck handler.

## Data model

Jobs retain canonical JSON payloads, outcomes, errors, attempt counters and scheduling information. The event table provides an ordered audit history. There is no retention deletion in this version, so operators must plan database growth. No secret is embedded in source or example data.
