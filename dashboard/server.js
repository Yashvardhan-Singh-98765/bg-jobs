'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { Client } = require('../src/client');
const { JobStore } = require('../src/store');
const { Worker } = require('../src/worker');

const rootDir = path.resolve(__dirname, '..');
const publicDir = path.join(__dirname, 'public');
const dbPath = process.env.JOBS_DB_PATH || path.join(rootDir, 'examples', 'demo-jobs.sqlite3');
const port = Number(process.env.PORT || 3100);

const store = new JobStore(dbPath);
const client = new Client({ store });
const worker = new Worker({ store, queues: null, concurrency: 3, pollIntervalMs: 100, backoff: () => 500 });
const execFileAsync = promisify(execFile);
const demoState = { running: false, lastRun: null, lastTests: null };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
worker.register('send_email', async (payload) => {
  await sleep(250);
  return { sent: true, to: payload.to || null };
});
worker.register('send_followup_email', async (payload) => ({ sent: true, to: payload.to || null, campaign: payload.campaign || null }));
worker.register('scrape_page', async (payload) => ({ url: payload.url || null, status: 'ok' }));
worker.register('flaky_report', async (payload, job) => {
  if (job.attempts < payload.successOnAttempt) throw new Error('temporary report failure');
  return { reportId: payload.reportId, generated: true };
});
worker.register('always_fails', async () => { throw new Error('intentional demo failure'); });

async function runDemo(kind) {
  if (demoState.running) throw new Error('A demonstration is already running.');
  demoState.running = true;
  const runId = Date.now();
  try {
    if (kind === 'scheduled') {
      client.push('send_followup_email', { to: 'future@example.com', campaign: 'two-day-demo' }, { delay: '2d', queue: 'scheduled' });
      client.push('send_followup_email', { to: 'soon@example.com', campaign: 'quick-demo' }, { delay: '3s', queue: 'scheduled', priority: 10 });
      demoState.lastRun = { kind, runId, message: 'Scheduled demo added a 2-day job and a 3-second job.' };
    } else {
      client.push('send_email', { to: 'demo@example.com' }, { queue: 'demo', priority: 1 });
      client.push('scrape_page', { url: 'https://example.com/products' }, { queue: 'demo' });
      client.push('flaky_report', { reportId: runId, successOnAttempt: 3 }, { queue: 'demo', retryLimit: 4 });
      client.push('always_fails', { reason: 'demo retry exhaustion' }, { queue: 'demo', retryLimit: 2 });
      client.push('send_email', { to: 'urgent@example.com' }, { queue: 'demo', priority: 20 });
      demoState.lastRun = { kind: 'standard', runId, message: 'Demo added concurrency, priority, retry, and failure jobs.' };
    }
  } finally {
    demoState.running = false;
  }
  return demoState.lastRun;
}

async function runTests() {
  const { stdout, stderr } = await execFileAsync(process.execPath, ['--test', path.join(rootDir, 'test', 'run.js')], { cwd: rootDir });
  return { output: `${stdout}${stderr}`.trim() };
}

const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
};

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (error) {
        reject(new Error('Invalid JSON body'));
      }
    });
    req.on('error', reject);
  });
}

function normalizeJob(row) {
  const payload = typeof row.payload === 'string' ? JSON.parse(row.payload) : (row.payload ?? {});
  return {
    id: row.id,
    queue: row.queue,
    type: row.type,
    payload,
    status: row.status,
    priority: row.priority,
    attempts: row.attempts,
    retry_limit: row.retry_limit,
    scheduled_at: row.scheduled_at,
    created_at: row.created_at,
    started_at: row.started_at,
    completed_at: row.completed_at,
    last_error: row.last_error,
    result: row.result ? JSON.parse(row.result) : null,
  };
}

function listJobs({ status = 'all', limit = 25 } = {}) {
  const safeLimit = Number(limit) || 25;
  let sql = 'SELECT * FROM jobs';
  const params = { limit: safeLimit };

  if (status && status !== 'all') {
    sql += ' WHERE status = @status';
    params.status = status;
  }

  sql += ' ORDER BY created_at DESC LIMIT @limit';

  return store.db.prepare(sql).all(params).map(normalizeJob);
}

function getQueueBreakdown() {
  const rows = store.db.prepare('SELECT queue, COUNT(*) AS total FROM jobs GROUP BY queue ORDER BY total DESC').all();
  return rows.map((row) => ({ queue: row.queue, total: row.total }));
}

function getOverview() {
  const stats = store.stats();
  const queueBreakdown = getQueueBreakdown();
  const attemptSummary = store.db.prepare('SELECT COALESCE(AVG(attempts), 0) AS average FROM jobs').get();
  return {
    totals: stats,
    queueBreakdown,
    averageAttempts: Number(attemptSummary.average || 0),
    generatedAt: new Date().toISOString(),
  };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');

  if (req.method === 'GET' && url.pathname === '/api/health') {
    return sendJson(res, 200, { ok: true, dbPath, worker: worker._running ? 'running' : 'stopped', demo: demoState });
  }

  if (req.method === 'GET' && url.pathname === '/api/stats') {
    return sendJson(res, 200, getOverview());
  }

  if (req.method === 'GET' && url.pathname === '/api/jobs') {
    const status = url.searchParams.get('status') || 'all';
    const limit = url.searchParams.get('limit') || '25';
    return sendJson(res, 200, { jobs: listJobs({ status, limit }) });
  }

  if (req.method === 'POST' && url.pathname === '/api/jobs') {
    try {
      const body = await readRequestBody(req);
      const type = String(body.type || '').trim();
      if (!type) {
        return sendJson(res, 400, { error: 'Job type is required.' });
      }

      const queue = String(body.queue || 'default').trim() || 'default';
      const payload = body.payload && typeof body.payload === 'object' ? body.payload : {};
      const priority = Number(body.priority ?? 0);
      const retryLimit = Number(body.retryLimit ?? 3);
      const delay = body.delay ?? null;
      const jobId = client.push(type, payload, {
        queue,
        priority,
        retryLimit,
        ...(delay ? { delay } : {}),
      });

      return sendJson(res, 201, { ok: true, jobId });
    } catch (error) {
      return sendJson(res, 500, { error: error.message || 'Could not enqueue job.' });
    }
  }

  if (req.method === 'GET' && url.pathname === '/api/queues') {
    return sendJson(res, 200, { queues: getQueueBreakdown() });
  }

  if (req.method === 'POST' && url.pathname === '/api/demos') {
    try {
      return sendJson(res, 201, { ok: true, demo: await runDemo(url.searchParams.get('kind') === 'scheduled' ? 'scheduled' : 'standard') });
    } catch (error) {
      return sendJson(res, 409, { error: error.message });
    }
  }

  if (req.method === 'POST' && url.pathname === '/api/tests') {
    try {
      const tests = await runTests();
      demoState.lastTests = { ok: true, ...tests };
      return sendJson(res, 200, demoState.lastTests);
    } catch (error) {
      demoState.lastTests = { ok: false, output: `${error.stdout || ''}${error.stderr || ''}`.trim() };
      return sendJson(res, 500, demoState.lastTests);
    }
  }

  if (req.method === 'POST' && url.pathname === '/api/reset') {
    if (demoState.running) {
      return sendJson(res, 409, { error: 'Stop the current demonstration before resetting history.' });
    }
    store.db.exec('DELETE FROM jobs');
    return sendJson(res, 200, { ok: true });
  }

  const requestedPath = url.pathname === '/' ? '/index.html' : url.pathname;
  const safePath = path.normalize(requestedPath).replace(/^\/+/, '');
  const filePath = path.join(publicDir, safePath);

  if (!filePath.startsWith(publicDir)) {
    return sendJson(res, 403, { error: 'Forbidden' });
  }

  fs.stat(filePath, (error, stats) => {
    if (error || !stats.isFile()) {
      const fallback = path.join(publicDir, 'index.html');
      fs.readFile(fallback, (readError, content) => {
        if (readError) {
          sendJson(res, 404, { error: 'Dashboard not found.' });
          return;
        }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(content);
      });
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const mimeType = mimeTypes[ext] || 'application/octet-stream';
    fs.readFile(filePath, (readError, content) => {
      if (readError) {
        sendJson(res, 500, { error: 'Could not read file.' });
        return;
      }
      res.writeHead(200, { 'Content-Type': mimeType });
      res.end(content);
    });
  });
});

worker.start().then(() => {
  server.listen(port, () => {
    console.log(`bg-jobs dashboard running at http://localhost:${port}`);
    console.log(`dashboard worker started with concurrency=${worker.concurrency}`);
  });
});

process.on('SIGINT', async () => {
  await worker.stop();
  client.close();
  process.exit(0);
});
