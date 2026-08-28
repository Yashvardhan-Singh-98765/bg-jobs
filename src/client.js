'use strict';

const { JobStore } = require('./store');
const { parseDuration } = require('./duration');

/**
 * Client is the producer-side API: the thing your web request handler,
 * cron, etc. calls to push work onto the queue and return immediately.
 *
 * const client = new Client({ dbPath: 'jobs.sqlite3' });
 * await client.push('send_email', { to: 'a@b.com' });
 * await client.push('cleanup_report', { userId: 42 }, { delay: '2d', retryLimit: 5 });
 */
class Client {
  /**
   * @param {object} [opts]
   * @param {string} [opts.dbPath] Path to the SQLite file backing the queue.
   *   Client and Worker must point at the same file to talk to each other.
   * @param {JobStore} [opts.store] Reuse an existing store (mainly for tests
   *   / running client and worker in the same process).
   */
  constructor({ dbPath = 'jobs.sqlite3', store } = {}) {
    this.store = store ?? new JobStore(dbPath);
    this._ownsStore = !store;
  }

  /**
   * Enqueue a job.
   *
   * @param {string} type A name identifying which handler should run this
   *   job (registered on the Worker with `worker.register(type, fn)`).
   * @param {object} [payload] Arbitrary JSON-serializable data passed to the handler.
   * @param {object} [options]
   * @param {string} [options.queue='default'] Which queue this job belongs to.
   * @param {number} [options.priority=0] Higher-priority jobs are claimed first.
   * @param {number} [options.retryLimit=3] Max attempts before giving up.
   * @param {number|string} [options.delay] Run this far in the future
   *   instead of immediately, e.g. `5000` (ms) or `'2d'`.
   * @param {Date} [options.runAt] Run at this exact time. Overrides `delay`.
   * @returns {number} The new job's id.
   */
  push(type, payload = {}, options = {}) {
    if (!type || typeof type !== 'string') {
      throw new TypeError('push(type, payload, options): `type` must be a non-empty string');
    }
    const {
      queue = 'default',
      priority = 0,
      retryLimit = 3,
      delay,
      runAt,
    } = options;

    let scheduledAt;
    if (runAt instanceof Date) {
      scheduledAt = runAt;
    } else if (delay !== undefined) {
      scheduledAt = new Date(Date.now() + parseDuration(delay));
    } else {
      scheduledAt = new Date();
    }

    return this.store.enqueue({
      queue,
      type,
      payload,
      priority,
      retryLimit,
      scheduledAt,
    });
  }

  /** Look up a single job by id (useful for polling job status/result). */
  getJob(id) {
    return this.store.getJob(id);
  }

  /** Counts of jobs per status; pass a queue name to scope it. */
  stats(queue) {
    return this.store.stats(queue);
  }

  close() {
    if (this._ownsStore) this.store.close();
  }
}

module.exports = { Client };
