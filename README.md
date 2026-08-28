# bg-jobs

A small background job system: a client library that pushes jobs onto a
queue, and a worker server that pulls jobs off it and runs them
concurrently, with retries and scheduled ("run this later") jobs.

Zero external dependencies - storage is SQLite via Node's built-in
`node:sqlite` module, so `npm install` isn't even required. Requires
**Node.js 22.5+** (uses the experimental `node:sqlite` module; you'll see a
one-line `ExperimentalWarning` on startup, which is expected).

## Quick start

```bash
npm run demo            # concurrency + retries + priority + scheduling, all in one
npm run demo:scheduled  # a focused look at "run this job N days later"
npm test                # automated test suite
```

`npm run demo` pushes a batch of jobs - a couple of plain ones, one that
fails twice before succeeding, one that fails forever, one scheduled a few
seconds out, and one high-priority job - then starts a worker with
`concurrency: 3` and logs every state transition with a timestamp and lane
number, so you can watch jobs run in parallel and see retries happen live.

## Using it in your own code

```js
const { Client, Worker } = require('bg-jobs');

// --- producer side (e.g. an API route) ---
const client = new Client({ dbPath: 'jobs.sqlite3' });

client.push('send_email', { to: 'user@example.com' });

client.push('send_followup_email', { to: 'user@example.com' }, {
  delay: '2d',        // or: runAt: new Date('2026-09-01T09:00:00Z')
  retryLimit: 5,
  priority: 0,
  queue: 'emails',     // optional - defaults to "default"
});

// --- consumer side (a separate worker process) ---
const worker = new Worker({
  dbPath: 'jobs.sqlite3', // same file as the client
  concurrency: 5,          // at most 5 jobs run at once
  queues: ['default', 'emails'],
});

worker.register('send_email', async (payload) => {
  await sendEmail(payload.to);
});
worker.register('send_followup_email', async (payload) => {
  await sendEmail(payload.to);
});

await worker.start();
// ... process stays alive, running jobs as they become due ...
// await worker.stop() for graceful shutdown (waits for in-flight jobs)
```

A job handler that throws is automatically retried (with backoff) up to
`retryLimit` times, then marked `failed`. See `worker.on(...)` events
(`jobStart`, `jobComplete`, `jobRetry`, `jobFailed`) for observability -
that's how the demo scripts produce their logs without any of that logic
living inside the library itself.

## How it works

**Storage / schema.** Everything lives in one `jobs` table (see
`src/store.js`): id, queue, type, JSON payload, status
(`pending → running → completed`, or `→ failed`), priority, attempts,
retry_limit, and `scheduled_at`. An index on
`(status, queue, scheduled_at, priority)` keeps the "find the next runnable
job" query fast even with a large backlog.

**Concurrency limit.** A `Worker` runs a fixed pool of `concurrency`
independent async loops ("lanes"). Each lane repeats: claim one job, `await`
its handler to completion, claim the next. Since a lane only ever has one
job in flight, `concurrency` lanes means at most `concurrency` jobs run at
once - that's the whole mechanism, no semaphore library needed.

**Atomic claiming (no double-processing).** Claiming a job is a single SQL
statement:

```sql
UPDATE jobs SET status = 'running', attempts = attempts + 1, started_at = ?
WHERE id = (
  SELECT id FROM jobs
  WHERE status = 'pending' AND scheduled_at <= ? AND queue IN (...)
  ORDER BY priority DESC, scheduled_at ASC, id ASC
  LIMIT 1
)
RETURNING *
```

Selecting and locking the row happen in one atomic statement, so even if
several lanes (or, since SQLite serializes writers, several processes) try
to claim at the same moment, each pending job is only ever handed to one of
them. `test/run.js` has a test that fires 30 jobs across 5 lanes and asserts
none of them is ever claimed twice.

**Retries.** On failure, if `attempts < retry_limit` the job goes back to
`pending` with `scheduled_at` pushed into the future by a backoff function
(default: exponential, `2^attempts` seconds capped at 5 minutes, plus up to
20% jitter so a burst of failures doesn't retry in lockstep). Once
`attempts` reaches `retry_limit`, the job is marked `failed` permanently.
Because retries just re-set `scheduled_at`, they reuse the exact same
mechanism as scheduling.

**Scheduled jobs.** `client.push(type, payload, { delay: '2d' })` (or
`{ runAt: someDate }`) simply sets `scheduled_at` in the future.
The claim query's `scheduled_at <= now()` filter means the job is invisible
to workers until that time arrives - no separate scheduler process
required. `examples/scheduled-demo.js` demonstrates this directly: it
schedules one job 2 days out and one 2 seconds out, and shows the former
stays `pending` untouched while the latter fires on time.

**Priority.** Jobs are claimed highest-`priority`-first, then oldest
`scheduled_at`, then lowest id - so within a priority tier it's FIFO.

## Design notes / limitations

- **SQLite + `node:sqlite` is intentionally the whole storage layer.** It
  makes the whole project runnable with no setup and no dependencies, which
  is ideal for a demo/take-home project. `node:sqlite` is still
  experimental in Node 22 - worth knowing before using this beyond a demo.
- **Single-process by design.** Multiple lanes give you real concurrency
  within one Node process, and SQLite's file locking means you *can*
  point several `Worker` processes at the same `.sqlite3` file, but SQLite
  isn't built for heavy multi-writer, multi-machine workloads. For a
  production system you'd swap `src/store.js` for Postgres (`SELECT ... FOR
  UPDATE SKIP LOCKED`, same atomic-claim idea) or Redis (e.g. `BRPOPLPUSH` /
  a Lua script) and run many worker processes/machines against it - the
  `Client`/`Worker` API in front of it wouldn't need to change.
- **At-least-once execution.** If a worker process crashes mid-job, that
  job is left `running` forever (no heartbeat/reaper is implemented). A
  production version would want a "reap jobs stuck in `running` past a
  timeout" sweep.
- **In-process concurrency, not OS parallelism.** Lanes are `async`
  functions on one event loop, so this is ideal for I/O-bound jobs (network
  calls, DB queries) like the ones in the examples. CPU-bound jobs would
  want `worker_threads` or separate processes to get real parallelism.

## Project layout

```
src/
  store.js    - SQLite schema + atomic claim/complete/retry/fail queries
  duration.js - parses "2d", "1h30m", etc. for the `delay` option
  client.js   - producer-side API: push(), getJob(), stats()
  worker.js   - consumer-side API: register(), start()/stop(), lifecycle events
  index.js    - public exports
examples/
  demo.js            - concurrency + retries + priority + scheduling together
  scheduled-demo.js  - focused look at delayed/scheduled jobs
test/
  run.js      - automated tests (concurrency limit, retries, atomicity, priority, scheduling)
```
