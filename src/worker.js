'use strict';

const { EventEmitter } = require('node:events');
const { JobStore } = require('./store');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Default backoff: 2^attempts seconds, capped at 5 minutes, plus up to 20%
 * random jitter so a burst of failures doesn't retry in lockstep.
 */
function defaultBackoff(attempts) {
  const base = Math.min(2 ** attempts * 1000, 5 * 60 * 1000);
  const jitter = base * 0.2 * Math.random();
  return base + jitter;
}

/**
 * Worker is the consumer-side "server": it polls the shared store for
 * runnable jobs and executes them against handlers you register by type.
 *
 * Concurrency is implemented as a fixed pool of independent async "lanes".
 * Each lane loops: claim a job -> await its handler -> claim the next one.
 * Because claiming is a single atomic SQL statement (see store.js), lanes
 * never race each other for the same row, and at most `concurrency` jobs
 * are ever in flight at once - which is exactly the concurrency limit the
 * spec asks for.
 *
 * const worker = new Worker({ dbPath: 'jobs.sqlite3', concurrency: 5 });
 * worker.register('send_email', async (payload) => { ... });
 * await worker.start();
 * // later
 * await worker.stop();
 */
class Worker extends EventEmitter {
  /**
   * @param {object} [opts]
   * @param {string} [opts.dbPath='jobs.sqlite3']
   * @param {JobStore} [opts.store] Reuse an existing store instead of opening dbPath.
   * @param {number} [opts.concurrency=4] Max number of jobs running at once.
   * @param {string[]} [opts.queues=['default']] Which queues to pull from.
   * @param {number} [opts.pollIntervalMs=200] How often an idle lane checks for new work.
   * @param {(attempts:number)=>number} [opts.backoff] Custom retry backoff (ms), given the
   *   attempt number that just failed.
   */
  constructor({
    dbPath = 'jobs.sqlite3',
    store,
    concurrency = 4,
    queues = ['default'],
    pollIntervalMs = 200,
    backoff = defaultBackoff,
  } = {}) {
    super();
    this.store = store ?? new JobStore(dbPath);
    this._ownsStore = !store;
    this.concurrency = concurrency;
    this.queues = queues;
    this.pollIntervalMs = pollIntervalMs;
    this.backoff = backoff;
    this.handlers = new Map();
    this._running = false;
    this._lanes = [];
  }

  /** Register the function that runs jobs of a given `type`. */
  register(type, handler) {
    if (typeof handler !== 'function') {
      throw new TypeError(`Handler for "${type}" must be a function`);
    }
    this.handlers.set(type, handler);
    return this;
  }

  /** Start `concurrency` lanes pulling and running jobs. Resolves once started. */
  async start() {
    if (this._running) return;
    this._running = true;
    this.emit('start', { concurrency: this.concurrency, queues: this.queues });
    for (let i = 0; i < this.concurrency; i++) {
      this._lanes.push(this._runLane(i));
    }
  }

  /** Stop claiming new jobs and wait for in-flight jobs to finish. */
  async stop() {
    this._running = false;
    await Promise.all(this._lanes);
    this._lanes = [];
    if (this._ownsStore) this.store.close();
    this.emit('stop');
  }

  async _runLane(laneId) {
    while (this._running) {
      const job = this.store.claimNext(this.queues);
      if (!job) {
        await sleep(this.pollIntervalMs);
        continue;
      }
      await this._execute(job, laneId);
    }
  }

  async _execute(job, laneId) {
    const handler = this.handlers.get(job.type);
    this.emit('jobStart', { job, laneId });

    if (!handler) {
      const error = new Error(`No handler registered for job type "${job.type}"`);
      this._handleFailure(job, error, laneId);
      return;
    }

    try {
      const result = await handler(job.payload, job);
      this.store.markCompleted(job.id, result);
      this.emit('jobComplete', { job, result, laneId });
    } catch (error) {
      this._handleFailure(job, error, laneId);
    }
  }

  _handleFailure(job, error, laneId) {
    if (job.attempts < job.retry_limit) {
      const delayMs = this.backoff(job.attempts);
      const nextRun = new Date(Date.now() + delayMs);
      this.store.scheduleRetry(job.id, nextRun, error.message);
      this.emit('jobRetry', { job, error, nextRun, laneId });
    } else {
      this.store.markFailed(job.id, error.message);
      this.emit('jobFailed', { job, error, laneId });
    }
  }
}

module.exports = { Worker, defaultBackoff };
