'use strict';

const { DatabaseSync } = require('node:sqlite');
const path = require('node:path');
const fs = require('node:fs');

const SCHEMA = `
CREATE TABLE IF NOT EXISTS jobs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  queue         TEXT    NOT NULL DEFAULT 'default',
  type          TEXT    NOT NULL,
  payload       TEXT    NOT NULL DEFAULT '{}',
  status        TEXT    NOT NULL DEFAULT 'pending', -- pending | running | completed | failed
  priority      INTEGER NOT NULL DEFAULT 0,          -- higher runs first
  attempts      INTEGER NOT NULL DEFAULT 0,
  retry_limit   INTEGER NOT NULL DEFAULT 3,
  scheduled_at  TEXT    NOT NULL,                     -- ISO string; job is eligible once now >= scheduled_at
  created_at    TEXT    NOT NULL,
  started_at    TEXT,
  completed_at  TEXT,
  last_error    TEXT,
  result        TEXT
);

-- Speeds up the "claim next job" query, which filters by status/queue and
-- orders by priority then schedule time.
CREATE INDEX IF NOT EXISTS idx_jobs_claim
  ON jobs (status, queue, scheduled_at, priority);
`;

/**
 * JobStore wraps a single SQLite connection (node:sqlite's DatabaseSync) and
 * exposes the small set of operations a client/worker need. All mutations
 * that must be atomic (claiming a job, retiring it) are single SQL
 * statements using `UPDATE ... WHERE id = (SELECT ...) RETURNING *`, so a
 * job can never be claimed twice even when several workers poll at once.
 */
class JobStore {
  constructor(dbPath = 'jobs.sqlite3') {
    const resolved = dbPath === ':memory:' ? dbPath : path.resolve(dbPath);
    if (resolved !== ':memory:') {
      fs.mkdirSync(path.dirname(resolved), { recursive: true });
    }
    this.db = new DatabaseSync(resolved);
    // WAL mode lets readers (e.g. a stats dashboard) run without blocking
    // on writers, and is generally the right default for a job queue.
    this.db.exec('PRAGMA journal_mode = WAL;');
    this.db.exec('PRAGMA busy_timeout = 5000;');
    this.db.exec(SCHEMA);

    this._insertStmt = this.db.prepare(`
      INSERT INTO jobs (queue, type, payload, priority, retry_limit, scheduled_at, created_at)
      VALUES (@queue, @type, @payload, @priority, @retry_limit, @scheduled_at, @created_at)
    `);

    // Atomically pick the highest-priority, oldest eligible pending job in
    // one of the given queues and flip it to "running" in the same
    // statement. If two workers race, SQLite serializes the writes so only
    // one of them gets a given row back; the other sees zero rows changed.
    this._claimStmt = this.db.prepare(`
      UPDATE jobs
      SET status = 'running', attempts = attempts + 1, started_at = @now
      WHERE id = (
        SELECT id FROM jobs
        WHERE status = 'pending'
          AND scheduled_at <= @now
          AND queue IN (SELECT value FROM json_each(@queues))
        ORDER BY priority DESC, scheduled_at ASC, id ASC
        LIMIT 1
      )
      RETURNING *
    `);

    this._completeStmt = this.db.prepare(`
      UPDATE jobs SET status = 'completed', completed_at = @now, result = @result
      WHERE id = @id
    `);

    this._retryStmt = this.db.prepare(`
      UPDATE jobs SET status = 'pending', scheduled_at = @nextRun, last_error = @error
      WHERE id = @id
    `);

    this._failStmt = this.db.prepare(`
      UPDATE jobs SET status = 'failed', completed_at = @now, last_error = @error
      WHERE id = @id
    `);

    this._getStmt = this.db.prepare('SELECT * FROM jobs WHERE id = @id');
  }

  enqueue({ queue, type, payload, priority, retryLimit, scheduledAt }) {
    const now = new Date().toISOString();
    const info = this._insertStmt.run({
      queue,
      type,
      payload: JSON.stringify(payload ?? {}),
      priority,
      retry_limit: retryLimit,
      scheduled_at: scheduledAt.toISOString(),
      created_at: now,
    });
    return Number(info.lastInsertRowid);
  }

  /** Claim the next runnable job from one of `queues`, or null if none is ready. */
  claimNext(queues) {
    const row = this._claimStmt.get({
      now: new Date().toISOString(),
      queues: JSON.stringify(queues),
    });
    if (!row) return null;
    return { ...row, payload: JSON.parse(row.payload) };
  }

  markCompleted(id, result) {
    this._completeStmt.run({
      id,
      now: new Date().toISOString(),
      result: result === undefined ? null : JSON.stringify(result),
    });
  }

  /** Re-queue a job for a future attempt. */
  scheduleRetry(id, nextRun, error) {
    this._retryStmt.run({
      id,
      nextRun: nextRun.toISOString(),
      error: String(error),
    });
  }

  /** Give up on a job permanently (attempts exhausted). */
  markFailed(id, error) {
    this._failStmt.run({
      id,
      now: new Date().toISOString(),
      error: String(error),
    });
  }

  getJob(id) {
    const row = this._getStmt.get({ id });
    if (!row) return null;
    return { ...row, payload: JSON.parse(row.payload) };
  }

  /** Counts of jobs per status, optionally scoped to one queue. */
  stats(queue) {
    const rows = queue
      ? this.db.prepare('SELECT status, COUNT(*) AS n FROM jobs WHERE queue = ? GROUP BY status').all(queue)
      : this.db.prepare('SELECT status, COUNT(*) AS n FROM jobs GROUP BY status').all();
    const out = { pending: 0, running: 0, completed: 0, failed: 0 };
    for (const r of rows) out[r.status] = r.n;
    return out;
  }

  close() {
    this.db.close();
  }
}

module.exports = { JobStore };
