'use strict';

/**
 * Demo: focuses specifically on scheduled jobs - the "run this 2 days
 * later" requirement. Pushes one job scheduled far in the future (using
 * the same API you'd use for a real 2-day delay) and one scheduled just a
 * couple of seconds out so we can actually observe it fire.
 *
 * Run with: npm run demo:scheduled
 */

const fs = require('node:fs');
const path = require('node:path');
const { Client } = require('../src/client');
const { Worker } = require('../src/worker');

const DB_PATH = path.join(__dirname, 'scheduled-demo-jobs.sqlite3');
for (const suffix of ['', '-wal', '-shm']) {
  fs.rmSync(DB_PATH + suffix, { force: true });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const client = new Client({ dbPath: DB_PATH });

  // Real-world usage: schedule something two days out.
  const farFutureId = client.push(
    'send_followup_email',
    { to: 'customer@example.com' },
    { delay: '2d' },
  );
  const farFutureJob = client.getJob(farFutureId);
  console.log(`Pushed job #${farFutureId}, scheduled_at = ${farFutureJob.scheduled_at}`);
  console.log('(that is ~2 days from now - it will NOT run during this demo)\n');

  // Demo-friendly: schedule something 2 seconds out so we can watch it fire.
  const soonId = client.push(
    'send_followup_email',
    { to: 'demo@example.com' },
    { delay: '2s' },
  );
  console.log(`Pushed job #${soonId}, scheduled 2 seconds out.`);

  console.log('\nStats immediately after pushing (nothing should be running yet):');
  console.log(client.stats());

  const worker = new Worker({ dbPath: DB_PATH, concurrency: 2, pollIntervalMs: 100 });
  worker.register('send_followup_email', async (payload) => {
    return { sent: true, to: payload.to };
  });
  worker.on('jobStart', ({ job }) => console.log(`\n-> job #${job.id} started at ${new Date().toISOString()}`));
  worker.on('jobComplete', ({ job }) => console.log(`-> job #${job.id} completed`));

  await worker.start();

  // Wait a bit longer than the 2s delay to prove the soon-job runs...
  await sleep(2500);

  console.log('\nStats after waiting ~2.5s (the 2-day job should still be pending):');
  console.log(client.stats());
  console.log(
    `Job #${farFutureId} status:`,
    client.getJob(farFutureId).status,
    '- untouched, because its scheduled_at is still two days away.',
  );

  await worker.stop();
  client.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
