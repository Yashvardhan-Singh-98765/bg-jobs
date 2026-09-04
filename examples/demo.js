'use strict';

/**
 * Demo: pushes a batch of jobs (some that succeed immediately, one that
 * fails a couple of times before succeeding, one that fails forever, and
 * one scheduled for a few seconds in the future) and runs a worker with a
 * concurrency limit of 3 to process them, logging the full lifecycle.
 *
 * Run with: npm run demo
 */

const fs = require('node:fs');
const path = require('node:path');
const { Client } = require('../src/client');
const { Worker } = require('../src/worker');

const DB_PATH = path.join(__dirname, 'demo-jobs.sqlite3');
// Start from a clean slate each run so the demo output is easy to read.
for (const suffix of ['', '-wal', '-shm']) {
  fs.rmSync(DB_PATH + suffix, { force: true });
}

const startedAt = Date.now();
const t = () => `+${((Date.now() - startedAt) / 1000).toFixed(2)}s`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const pick = (items) => items[randomInt(0, items.length - 1)];
const makeEmail = () => `user-${Math.random().toString(36).slice(2, 8)}@${pick(['example.com', 'demo.net', 'mail.test'])}`;
const makeUrl = (segment) => `https://example.com/${segment}/${randomInt(1000, 9999)}`;
const demoRunToken = `run-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

async function main() {
  const client = new Client({ dbPath: DB_PATH });

  console.log(`[${t()}] Enqueuing jobs...`);

  // A handful of normal jobs that just take a bit of simulated I/O time.
  client.push('send_email', { to: makeEmail() });
  client.push('send_email', { to: makeEmail() });
  client.push('scrape_page', { url: makeUrl(pick(['products', 'reviews', 'catalog'])) });
  client.push('scrape_page', { url: makeUrl(pick(['pricing', 'features', 'docs'])) });

  // A job that fails a few times before eventually succeeding.
  const flakyRetryLimit = randomInt(3, 5);
  const flakySuccessAttempt = randomInt(2, flakyRetryLimit);
  const flakyId = client.push(
    'flaky_report',
    { reportId: randomInt(1000, 9999), successOnAttempt: flakySuccessAttempt },
    { retryLimit: flakyRetryLimit },
  );

  // A job that will never succeed, with a randomized retry budget.
  const doomedRetryLimit = randomInt(2, 4);
  const doomedId = client.push(
    'always_fails',
    { reason: 'demo of retry exhaustion' },
    { retryLimit: doomedRetryLimit },
  );

  // A job scheduled for the future. In real usage this would be
  // { delay: '2d' } - here we use a few seconds so the demo finishes fast.
  const scheduledPriority = randomInt(0, 5);
  const scheduledId = client.push(
    'send_email',
    { to: makeEmail() },
    { delay: '3s', priority: scheduledPriority },
  );

  // A high-priority job pushed last, to show it still jumps the queue.
  const urgentPriority = randomInt(10, 20);
  const urgentId = client.push(
    'send_email',
    { to: makeEmail() },
    { priority: urgentPriority },
  );

  console.log(
    `[${t()}] run=${demoRunToken} Pushed jobs. flaky=${flakyId} doomed=${doomedId} ` +
      `scheduled=${scheduledId} (runs in 3s, priority ${scheduledPriority}) ` +
      `urgent=${urgentId} (priority ${urgentPriority}) retryLimit=${flakyRetryLimit}/${doomedRetryLimit}`,
  );

  // --- Worker setup -------------------------------------------------------

  const worker = new Worker({
    dbPath: DB_PATH,
    concurrency: 3, // at most 3 jobs run at the same time
    pollIntervalMs: 100,
    // Shorter backoff than the library default so the demo doesn't take
    // minutes: attempt 1 -> ~0.5s, attempt 2 -> ~1s, etc.
    backoff: (attempts) => attempts * 500,
  });

  worker.register('send_email', async (payload) => {
    await sleep(400 + Math.random() * 400);
    return {
      sent: true,
      to: payload.to,
      messageId: `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      bytes: randomInt(200, 1200),
    };
  });

  worker.register('scrape_page', async (payload) => {
    await sleep(600 + Math.random() * 400);
    return {
      url: payload.url,
      bytes: randomInt(5000, 50000),
      status: 'ok',
    };
  });

  worker.register('flaky_report', async (payload, job) => {
    await sleep(200);
    if (job.attempts < payload.successOnAttempt) {
      throw new Error(`transient failure generating report ${payload.reportId}`);
    }
    return {
      reportId: payload.reportId,
      rows: randomInt(80, 150),
      successOnAttempt: payload.successOnAttempt,
      generatedAt: new Date().toISOString(),
    };
  });

  worker.register('always_fails', async () => {
    await sleep(150);
    throw new Error('this job type is broken on purpose');
  });

  // --- Lifecycle logging ---------------------------------------------------

  worker.on('jobStart', ({ job, laneId }) => {
    console.log(
      `[${t()}] lane ${laneId} | START   #${job.id} ${job.type} (attempt ${job.attempts}, ` +
        `run=${demoRunToken})`,
    );
  });
  worker.on('jobComplete', ({ job, result, laneId }) => {
    console.log(`[${t()}] lane ${laneId} | DONE    #${job.id} ${job.type} => ${JSON.stringify(result)}`);
  });
  worker.on('jobRetry', ({ job, error, nextRun, laneId }) => {
    const inSec = ((nextRun - Date.now()) / 1000).toFixed(1);
    console.log(
      `[${t()}] lane ${laneId} | RETRY   #${job.id} ${job.type} ` +
        `(attempt ${job.attempts}/${job.retry_limit} failed: ${error.message}) -> retrying in ${inSec}s`,
    );
  });
  worker.on('jobFailed', ({ job, error, laneId }) => {
    console.log(
      `[${t()}] lane ${laneId} | FAILED  #${job.id} ${job.type} ` +
        `(gave up after ${job.attempts}/${job.retry_limit} attempts: ${error.message})`,
    );
  });

  await worker.start();
  console.log(`[${t()}] Worker started with concurrency=3\n`);

  // Wait until every job has reached a terminal state (or time out).
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    const stats = client.stats();
    if (stats.pending === 0 && stats.running === 0) break;
    await sleep(200);
  }

  await worker.stop();

  console.log(`\n[${t()}] Final stats:`, client.stats());
  console.log(`[${t()}] Doomed job record:`, client.getJob(doomedId));
  console.log(`[${t()}] Flaky job record:`, client.getJob(flakyId));

  client.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
