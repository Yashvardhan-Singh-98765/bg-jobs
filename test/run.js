'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { JobStore } = require('../src/store');
const { Client } = require('../src/client');
const { Worker } = require('../src/worker');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Waits until `check()` returns true or `ms` elapses, polling every 20ms.
async function waitUntil(check, ms = 5000) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await check()) return true;
    await sleep(20);
  }
  return false;
}

function makeHarness() {
  const store = new JobStore(':memory:');
  const client = new Client({ store });
  return { store, client };
}

test('never runs more jobs at once than the concurrency limit', async () => {
  const { store, client } = makeHarness();
  const worker = new Worker({ store, concurrency: 3, pollIntervalMs: 20 });

  let inFlight = 0;
  let maxObserved = 0;
  worker.register('work', async () => {
    inFlight++;
    maxObserved = Math.max(maxObserved, inFlight);
    await sleep(100);
    inFlight--;
  });

  for (let i = 0; i < 12; i++) client.push('work', { i });

  await worker.start();
  await waitUntil(() => {
    const s = client.stats();
    return s.pending === 0 && s.running === 0;
  });
  await worker.stop();

  assert.equal(client.stats().completed, 12);
  assert.ok(maxObserved <= 3, `expected at most 3 concurrent jobs, saw ${maxObserved}`);
  assert.equal(maxObserved, 3, 'expected the pool to actually reach the concurrency limit of 3');
});

test('retries a failing job up to its retry limit, then succeeds if the handler recovers', async () => {
  const { store, client } = makeHarness();
  const worker = new Worker({ store, concurrency: 1, pollIntervalMs: 20, backoff: () => 10 });

  worker.register('flaky', async (_payload, job) => {
    if (job.attempts < 3) throw new Error('not yet');
    return 'ok';
  });

  const id = client.push('flaky', {}, { retryLimit: 5 });
  await worker.start();
  await waitUntil(() => client.getJob(id).status !== 'pending' && client.getJob(id).status !== 'running');
  await worker.stop();

  const job = client.getJob(id);
  assert.equal(job.status, 'completed');
  assert.equal(job.attempts, 3);
  assert.equal(JSON.parse(job.result), 'ok');
});

test('permanently fails a job once its retry limit is exhausted', async () => {
  const { store, client } = makeHarness();
  const worker = new Worker({ store, concurrency: 1, pollIntervalMs: 20, backoff: () => 10 });

  worker.register('always_broken', async () => {
    throw new Error('nope');
  });

  const id = client.push('always_broken', {}, { retryLimit: 2 });
  await worker.start();
  await waitUntil(() => client.getJob(id).status === 'failed');
  await worker.stop();

  const job = client.getJob(id);
  assert.equal(job.status, 'failed');
  assert.equal(job.attempts, 2);
  assert.match(job.last_error, /nope/);
});

test('does not run a scheduled job before its scheduled time', async () => {
  const { store, client } = makeHarness();
  const worker = new Worker({ store, concurrency: 1, pollIntervalMs: 20 });
  worker.register('later', async () => 'done');

  const id = client.push('later', {}, { delay: 300 });
  await worker.start();

  await sleep(100);
  assert.equal(client.getJob(id).status, 'pending', 'job fired before its scheduled time');

  const ranInTime = await waitUntil(() => client.getJob(id).status === 'completed', 2000);
  await worker.stop();
  assert.ok(ranInTime, 'scheduled job never ran after its delay elapsed');
});

test('claims higher-priority jobs before lower-priority ones', async () => {
  const { store, client } = makeHarness();
  const worker = new Worker({ store, concurrency: 1, pollIntervalMs: 20 });

  const order = [];
  worker.register('prio', async (payload) => {
    order.push(payload.label);
    await sleep(20);
  });

  client.push('prio', { label: 'low' }, { priority: 0 });
  client.push('prio', { label: 'medium' }, { priority: 5 });
  client.push('prio', { label: 'high' }, { priority: 10 });

  await worker.start();
  await waitUntil(() => client.stats().completed === 3);
  await worker.stop();

  assert.deepEqual(order, ['high', 'medium', 'low']);
});

test('two lanes never claim the same job (atomic claim)', async () => {
  const { store, client } = makeHarness();
  const worker = new Worker({ store, concurrency: 5, pollIntervalMs: 5 });

  const seen = new Set();
  const duplicates = [];
  worker.register('once_only', async (_payload, job) => {
    if (seen.has(job.id)) duplicates.push(job.id);
    seen.add(job.id);
    await sleep(15);
  });

  for (let i = 0; i < 30; i++) client.push('once_only', { i });

  await worker.start();
  await waitUntil(() => client.stats().completed === 30);
  await worker.stop();

  assert.equal(duplicates.length, 0, `jobs claimed more than once: ${duplicates}`);
});
