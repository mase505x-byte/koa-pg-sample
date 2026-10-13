import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize, resolve, sep } from 'node:path';

// ============================================================
// In-memory equivalents of the tables.
// The program is single-threaded, so FOR UPDATE SKIP LOCKED is not needed:
// each claim runs synchronously and is therefore atomic.
// ============================================================
const scheduledJobs = new Map();      // job_id -> row
let jobIdSeq = 0;                     // SERIAL

const webhookDeliveries = new Map();  // delivery_id -> row
const deliveryKeys = new Map();       // `${job_id}|${scheduled_for_ms}` -> delivery_id (UNIQUE constraint)
let deliveryIdSeq = 0;                // BIGSERIAL

// ============================================================
// schedule_job(p_endpoint_url, p_cron_expr) -> INT
// ============================================================
export function scheduleJob(endpointUrl, cronExpr) {
  if (typeof endpointUrl !== 'string' || typeof cronExpr !== 'string') {
    throw new TypeError('endpointUrl and cronExpr must be strings (NOT NULL)');
  }
  const jobId = ++jobIdSeq;
  scheduledJobs.set(jobId, {
    job_id: jobId,
    endpoint_url: endpointUrl,
    cron_expr: cronExpr,
    is_active: true,
    max_attempts: 3,
    created_at: new Date(),
  });
  return jobId;
}

// ============================================================
// cancel_job(p_job_id INT) -> VOID
// ============================================================
export function cancelJob(jobId) {
  const job = scheduledJobs.get(jobId);
  if (job) job.is_active = false;
}

// ============================================================
// Cron matching helpers (RecurrenceRule equivalent)
// Timestamps are interpreted in UTC.
// ============================================================
export function cronFieldMatches(field, val) {
  // SQL NULL propagation: a missing field or value never matches.
  if (field == null || val == null) return false;
  return field === '*' || field.split(',').map(Number).includes(val);
}

export function cronMatches(expr, ts) {
  const parts = expr.split(' ');
  const values = [
    ts.getUTCSeconds(),          // SECOND
    ts.getUTCMinutes(),          // MINUTE
    ts.getUTCHours(),            // HOUR
    ts.getUTCDate(),             // DAY (day of month)
    ts.getUTCMonth() + 1,        // MONTH (1-12)
    ts.getUTCDay(),              // DOW (0 = Sunday)
  ];
  return values.every((val, i) => cronFieldMatches(parts[i], val));
}

// ============================================================
// enqueue_due_jobs(p_tick TIMESTAMP) -> INT
// ============================================================
export function enqueueDueJobs(tick) {
  let insertedCount = 0;

  for (const job of scheduledJobs.values()) {
    if (!job.is_active || !cronMatches(job.cron_expr, tick)) continue;

    const key = `${job.job_id}|${tick.getTime()}`;
    if (deliveryKeys.has(key)) continue; // ON CONFLICT (job_id, scheduled_for) DO NOTHING

    const deliveryId = ++deliveryIdSeq;
    const now = new Date();
    webhookDeliveries.set(deliveryId, {
      delivery_id: deliveryId,
      job_id: job.job_id,
      scheduled_for: tick,
      status: 'pending',
      attempts: 0,
      next_attempt_at: now,
      claimed_until: null,
      last_status_code: null,
      created_at: now,
    });
    deliveryKeys.set(key, deliveryId);
    insertedCount++;
  }

  return insertedCount;
}

// ============================================================
// claim_webhook_deliveries(p_limit INT DEFAULT 50)
// Claims pending deliveries, plus processing ones whose claim expired.
// Note: the provided SQL was truncated at "RETURNING d.deliver". The
// returned columns below are inferred from the declared RETURNS TABLE
// signature (delivery_id, endpoint_url, scheduled_for, attempt).
// ============================================================
export function claimWebhookDeliveries(limit = 50, now = new Date()) {
  const max = Math.max(limit, 0); // GREATEST(p_limit, 0)
  const claimedUntil = new Date(now.getTime() + 60_000); // INTERVAL '1 minute'

  const picked = [...webhookDeliveries.values()]
    .filter(d =>
      (d.status === 'pending' && d.next_attempt_at <= now) ||
      (d.status === 'processing' && d.claimed_until !== null && d.claimed_until <= now)
    )
    .sort((a, b) =>
      (a.next_attempt_at - b.next_attempt_at) || (a.delivery_id - b.delivery_id)
    )
    .slice(0, max);

  return picked.map(d => {
    d.status = 'processing';
    d.claimed_until = claimedUntil;
    return {
      delivery_id: d.delivery_id,
      endpoint_url: scheduledJobs.get(d.job_id).endpoint_url,
      scheduled_for: d.scheduled_for,
      attempt: d.attempts,
    };
  });
}

// ============================================================
// Static web server (not started automatically)
// ============================================================
const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon',
};

export function createStaticServer(rootDir) {
  const root = resolve(rootDir);

  return createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', 'http://localhost');
      let relPath = decodeURIComponent(url.pathname);
      if (relPath.endsWith('/')) relPath += 'index.html';

      const filePath = join(root, normalize(relPath));
      if (filePath !== root && !filePath.startsWith(root + sep)) {
        res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' }).end('Forbidden');
        return;
      }

      const body = await readFile(filePath);
      res.writeHead(200, {
        'Content-Type': MIME_TYPES[extname(filePath)] ?? 'application/octet-stream',
      });
      res.end(body);
    } catch {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }).end('Not Found');
    }
  });
}
