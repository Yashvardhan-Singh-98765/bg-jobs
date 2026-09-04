const statsEls = {
  pending: document.getElementById('pendingCount'),
  running: document.getElementById('runningCount'),
  completed: document.getElementById('completedCount'),
  failed: document.getElementById('failedCount'),
};

const totalJobsEl = document.getElementById('totalJobs');
const successRateEl = document.getElementById('successRate');
const avgAttemptsEl = document.getElementById('avgAttempts');
const queueBreakdownEl = document.getElementById('queueBreakdown');
const jobsTableBody = document.getElementById('jobsTableBody');
const activityList = document.getElementById('activityList');
const refreshBtn = document.getElementById('refreshBtn');
const runDemoBtn = document.getElementById('runDemoBtn');
const runScheduledBtn = document.getElementById('runScheduledBtn');
const runTestsBtn = document.getElementById('runTestsBtn');
const resetBtn = document.getElementById('resetBtn');
const systemMessage = document.getElementById('systemMessage');
const jobForm = document.getElementById('jobForm');
const statusFilter = document.getElementById('statusFilter');

function fmtDate(value) {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function getStatusTone(status) {
  if (status === 'completed') return 'success';
  if (status === 'failed') return 'danger';
  if (status === 'running') return 'primary';
  return 'warning';
}

function renderStats(stats) {
  const totals = stats || {};
  const pending = Number(totals.pending || 0);
  const running = Number(totals.running || 0);
  const completed = Number(totals.completed || 0);
  const failed = Number(totals.failed || 0);
  const total = pending + running + completed + failed;
  const finished = completed + failed;
  const successRate = finished === 0 ? 0 : (completed / finished) * 100;

  statsEls.pending.textContent = pending;
  statsEls.running.textContent = running;
  statsEls.completed.textContent = completed;
  statsEls.failed.textContent = failed;

  totalJobsEl.textContent = total;
  successRateEl.textContent = `${Math.round(successRate)}%`;
  avgAttemptsEl.textContent = Number(stats.averageAttempts || 0).toFixed(1);
}

function renderQueues(queueBreakdown) {
  if (!Array.isArray(queueBreakdown) || queueBreakdown.length === 0) {
    queueBreakdownEl.innerHTML = '<p class="empty-state">No queues yet.</p>';
    return;
  }

  queueBreakdownEl.innerHTML = queueBreakdown
    .map(
      (queue) => `
        <div class="queue-item">
          <div>
            <strong>${queue.queue}</strong>
            <div class="queue-meta">${queue.total} jobs queued</div>
          </div>
          <span class="queue-pill">${queue.total}</span>
        </div>
      `,
    )
    .join('');
}

function renderActivity(jobs) {
  const items = Array.isArray(jobs) ? jobs.slice(0, 6) : [];

  if (!items.length) {
    activityList.innerHTML = '<li class="activity-item"><span class="dot warning"></span><div><strong>No activity</strong><span>There are no jobs in the queue right now.</span></div></li>';
    return;
  }

  activityList.innerHTML = items
    .map((job) => {
      const tone = getStatusTone(job.status);
      const label = job.status === 'completed' ? 'Completed' : job.status === 'failed' ? 'Failed' : job.status === 'running' ? 'Running' : 'Queued';
      return `
        <li class="activity-item">
          <span class="dot ${tone === 'success' ? 'success' : tone === 'danger' ? 'danger' : tone === 'primary' ? '' : 'warning'}"></span>
          <div>
            <strong>#${job.id} · ${job.type}</strong>
            <span>${label} in ${job.queue} · ${fmtDate(job.created_at)}</span>
          </div>
        </li>
      `;
    })
    .join('');
}

function renderJobs(jobs) {
  if (!Array.isArray(jobs) || jobs.length === 0) {
    jobsTableBody.innerHTML = '<tr><td colspan="7">No jobs found.</td></tr>';
    return;
  }

  jobsTableBody.innerHTML = jobs
    .slice(0, 25)
    .map(
      (job) => `
        <tr>
          <td>#${job.id}</td>
          <td>${job.type}</td>
          <td>${job.queue}</td>
          <td><span class="status-badge ${job.status}">${job.status}</span></td>
          <td>${job.priority}</td>
          <td>${job.attempts}/${job.retry_limit}</td>
          <td>${fmtDate(job.scheduled_at)}</td>
        </tr>
      `,
    )
    .join('');
}

async function loadDashboard() {
  try {
    const status = statusFilter.value || 'all';
    const [statsRes, jobsRes] = await Promise.all([
      fetch('/api/stats'),
      fetch(`/api/jobs?status=${encodeURIComponent(status)}&limit=25`),
    ]);

    const statsData = await statsRes.json();
    const jobsData = await jobsRes.json();

    renderStats({ ...statsData.totals, averageAttempts: statsData.averageAttempts });
    renderQueues(statsData.queueBreakdown || []);
    renderJobs(jobsData.jobs || []);
    renderActivity(jobsData.jobs || []);
    const health = await fetch('/api/health').then((response) => response.json());
    systemMessage.textContent = `Worker ${health.worker || 'unknown'} · updates every 5 seconds`;
  } catch (error) {
    console.error('Dashboard load failed:', error);
    systemMessage.textContent = 'Dashboard connection failed';
  }
}

async function runAction(button, url, successMessage) {
  button.disabled = true;
  systemMessage.textContent = 'Running...';
  try {
    const response = await fetch(url, { method: 'POST' });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'Action failed.');
    systemMessage.textContent = successMessage(result);
    await loadDashboard();
  } catch (error) {
    systemMessage.textContent = error.message;
  } finally {
    button.disabled = false;
  }
}

jobForm.addEventListener('submit', async (event) => {
  event.preventDefault();

  const formData = new FormData(jobForm);
  const payloadText = formData.get('payload');

  let payload = {};
  try {
    payload = payloadText ? JSON.parse(payloadText) : {};
  } catch (error) {
    alert('Payload must be valid JSON.');
    return;
  }

  const body = {
    type: formData.get('type'),
    queue: formData.get('queue'),
    priority: Number(formData.get('priority') ?? 0),
    retryLimit: Number(formData.get('retryLimit') ?? 3),
    delay: formData.get('delay') || undefined,
    payload,
  };

  const response = await fetch('/api/jobs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  const result = await response.json();
  if (!response.ok) {
    alert(result.error || 'Could not enqueue job.');
    return;
  }

  jobForm.reset();
  jobForm.type.value = 'send_email';
  jobForm.queue.value = 'default';
  jobForm.priority.value = 0;
  jobForm.retryLimit.value = 3;
  jobForm.payload.value = '{"to":"user@example.com"}';

  await loadDashboard();
  alert(`Job enqueued with id ${result.jobId}`);
});

refreshBtn.addEventListener('click', loadDashboard);
statusFilter.addEventListener('change', loadDashboard);
runDemoBtn.addEventListener('click', () => runAction(runDemoBtn, '/api/demos', () => 'Demo jobs added. Watch their statuses update live.'));
runScheduledBtn.addEventListener('click', () => runAction(runScheduledBtn, '/api/demos?kind=scheduled', () => 'Scheduled jobs added. The quick job runs in about 3 seconds.'));
runTestsBtn.addEventListener('click', () => runAction(runTestsBtn, '/api/tests', (result) => result.ok ? 'All automated tests passed.' : 'Automated tests failed.'));
resetBtn.addEventListener('click', async () => {
  if (!confirm('Delete all job history from this dashboard database?')) return;
  await runAction(resetBtn, '/api/reset', () => 'Job history reset.');
});

loadDashboard();
setInterval(loadDashboard, 5000);
