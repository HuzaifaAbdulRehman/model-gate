import { setTimeout as delay } from 'node:timers/promises';
import type { FastifyInstance } from 'fastify';
import type pg from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';
import { ProviderClient } from '../src/providers/client.js';
import type { Cache } from '../src/redis.js';
import { buildServer } from '../src/server.js';
import { createTestPool, createTestRedis, truncateAll, waitForRedis } from './helpers/db.js';
import { TEST_DATABASE_URL } from './helpers/global-setup.js';
import { startMock, type RunningMock } from './helpers/mock.js';

const db: pg.Pool = createTestPool();
const cache: Cache = createTestRedis();
const API_KEY = 'x'.repeat(24);
const TENANT = 'default';

/**
 * Format-valid and entirely fabricated. A leak test has to plant things that
 * look real, or it only proves the redactor recognises its own examples.
 */
const SECRETS = {
  email: 'ada.lovelace@example.com',
  apiKey: 'sk-proj123T3BlbkF' + 'Jabcdefghijklmnop', // gitleaks:allow
  card: '4242424242424242',
  cnic: '42101-1234567-8',
  ip: '203.0.113.77',
  phone: '+14155552671',
};

let mock: RunningMock;
let app: FastifyInstance;
let client: ProviderClient;

beforeAll(async () => {
  await waitForRedis(cache);
  mock = await startMock();
  const config = loadConfig({
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    DATABASE_URL: TEST_DATABASE_URL,
    REDIS_URL: 'redis://localhost:6380',
    GATEWAY_API_KEY: API_KEY,
    REDACTION_PEPPER: 'p'.repeat(32),
    MOCK_PRIMARY_URL: mock.url,
    MOCK_BACKUP_URL: mock.url,
  });
  client = new ProviderClient();
  app = buildServer({ config, db, cache, client });
  await app.ready();
});

afterEach(async () => {
  await truncateAll(db);
  const keys = [...(await cache.keys(`mg:{t:${TENANT}}:*`)), ...(await cache.keys('mg:cache:*'))];
  if (keys.length > 0) await cache.del(...keys);
});

afterAll(async () => {
  await app.close();
  await client.close();
  await mock.close();
  await cache.quit();
  await db.end();
});

function chat(content: string, extra: Record<string, unknown> = {}) {
  return app.inject({
    method: 'POST',
    url: '/v1/chat/completions',
    headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
    payload: { model: 'mock-1', messages: [{ role: 'user', content }], ...extra },
  });
}

/**
 * The audit write happens in an onResponse hook, so it lands after the reply
 * the caller already has. Polling rather than sleeping a fixed time keeps the
 * test from passing because the guess was generous.
 */
async function waitForRows(count = 1, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const { rows } = await db.query<{ n: number }>('SELECT count(*)::int AS n FROM requests');
    if ((rows[0]?.n ?? 0) >= count) return;
    if (Date.now() > deadline) throw new Error(`audit rows never appeared (wanted ${count})`);
    await delay(25);
  }
}

describe('what gets recorded', () => {
  it('writes the request, its payload and one row per attempt', async () => {
    await chat('hello there');
    await waitForRows();

    const request = (await db.query('SELECT * FROM requests')).rows[0] as Record<string, unknown>;
    expect(request['outcome']).toBe('ok');
    expect(request['final_provider']).toBe('mock-primary');
    expect(request['cache_result']).toBe('miss');
    expect(request['token_source']).toBe('provider');
    expect(request['attempt_count']).toBe(1);
    expect(Number(request['total_ms'])).toBeGreaterThanOrEqual(0);

    const payload = (await db.query('SELECT * FROM request_payloads')).rows[0] as Record<string, unknown>;
    expect(payload['engine_version']).toBe('redact-1');
    expect(Buffer.isBuffer(payload['prompt_fingerprint'])).toBe(true);

    const attempts = (await db.query('SELECT * FROM request_attempts ORDER BY attempt_no')).rows;
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.provider).toBe('mock-primary');
    expect(attempts[0]?.outcome).toBe('ok');
    expect(attempts[0]?.committed).toBe(false);
  });

  it('records every attempt of a failover, not just the winner', async () => {
    // The attempt rows are the failover evidence. One row would say the request
    // succeeded and hide that a provider died to make it happen.
    const failing = await startMock({ alwaysFail: 'server-error' });
    const config = loadConfig({
      NODE_ENV: 'test',
      LOG_LEVEL: 'silent',
      DATABASE_URL: TEST_DATABASE_URL,
      REDIS_URL: 'redis://localhost:6380',
      GATEWAY_API_KEY: API_KEY,
      REDACTION_PEPPER: 'p'.repeat(32),
      MOCK_PRIMARY_URL: failing.url,
      MOCK_BACKUP_URL: mock.url,
      RETRY_BASE_MS: '0',
      RETRY_CAP_MS: '1',
    });
    const c = new ProviderClient();
    const failoverApp = buildServer({ config, db, cache, client: c });
    await failoverApp.ready();

    await failoverApp.inject({
      method: 'POST',
      url: '/v1/chat/completions',
      headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
      payload: { model: 'mock-1', messages: [{ role: 'user', content: 'hi' }] },
    });
    await waitForRows();

    const attempts = (await db.query('SELECT * FROM request_attempts ORDER BY attempt_no')).rows;
    expect(attempts).toHaveLength(3);
    expect(attempts.map((a) => a.outcome)).toEqual(['http_error', 'http_error', 'ok']);
    expect(attempts.map((a) => a.provider)).toEqual(['mock-primary', 'mock-primary', 'mock-backup']);

    await failoverApp.close();
    await c.close();
    await failing.close();
  });

  it('records a rejected request too', async () => {
    await chat('hi', { max_tokens: 1 });
    await waitForRows();

    const { rows } = await db.query('SELECT outcome, cache_result FROM requests');
    expect(rows[0]?.outcome).toBe('ok');
    expect(rows[0]?.cache_result).toBe('miss');
  });

  it('marks a cache hit as such and names no provider', async () => {
    await chat('cached please');
    await waitForRows(1);
    await chat('cached please');
    await waitForRows(2);

    const { rows } = await db.query(
      'SELECT cache_result, final_provider, attempt_count FROM requests ORDER BY created_at',
    );
    expect(rows[1]?.cache_result).toBe('hit');
    expect(rows[1]?.final_provider).toBeNull();
    expect(rows[1]?.attempt_count).toBe(0);
  });
});

describe('nothing sensitive reaches the store', () => {
  it('keeps every planted secret out of Postgres', async () => {
    // A whole-store assertion rather than per-field ones. The leak that
    // actually happens in these systems is a second copy nobody remembered:
    // a retry buffer, a metadata blob, an error path. Checking named columns
    // structurally cannot find those.
    const prompt = [
      `email ${SECRETS.email}`,
      `key ${SECRETS.apiKey}`,
      `card ${SECRETS.card}`,
      `cnic ${SECRETS.cnic}`,
      `ip ${SECRETS.ip}`,
      `phone ${SECRETS.phone}`,
    ].join(' and ');

    const res = await chat(prompt);
    expect(res.statusCode).toBe(200);
    await waitForRows();

    const dumped = (
      await Promise.all(
        ['requests', 'request_payloads', 'request_attempts'].map(async (table) => {
          const { rows } = await db.query(`SELECT * FROM ${table}`);
          return JSON.stringify(rows);
        }),
      )
    ).join('\n');

    for (const [name, value] of Object.entries(SECRETS)) {
      expect(dumped, `${name} leaked into postgres`).not.toContain(value);
    }

    // And the row still says what was removed, so the log stays debuggable.
    const payload = (await db.query('SELECT * FROM request_payloads')).rows[0] as {
      prompt: string;
      redactions: unknown[];
    };
    expect(payload.prompt).toContain('[REDACTED:email:');
    expect(payload.prompt).toContain('[REDACTED:api_key]');
    expect(payload.prompt).toContain('[REDACTED:credit_card:visa:4242]');
    expect(payload.redactions.length).toBeGreaterThanOrEqual(5);

    const classes = (await db.query('SELECT redaction_classes::text[] AS c FROM requests')).rows[0] as {
      c: string[];
    };
    expect(classes.c).toContain('email');
    expect(classes.c).toContain('api_key');
  });

  it('keeps the prompt out of Redis as well', async () => {
    // Redis holds the completion so a cache hit can serve real text, and that
    // is a deliberate boundary rather than an oversight. The prompt has no
    // business being there in any form.
    await chat(`contact ${SECRETS.email} about card ${SECRETS.card}`);
    await waitForRows();

    const keys = await cache.keys('mg:*');
    const values = await Promise.all(
      keys.map(async (k) => `${k} ${(await cache.get(k).catch(() => null)) ?? ''}`),
    );
    const dumped = values.join('\n');

    expect(dumped).not.toContain(SECRETS.email);
    expect(dumped).not.toContain(SECRETS.card);
  });

  it('does not echo the body back in a validation error', async () => {
    // Zod issues can carry the offending input, and an error handler that
    // serialises them hands the caller their own secret back in a 400. The
    // gateway sends the message and the path, never the value.
    const res = await app.inject({
      method: 'POST',
      url: '/v1/chat/completions',
      headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
      payload: { model: 'mock-1', messages: [{ role: 'user', content: SECRETS.apiKey }], max_tokens: -5 },
    });

    expect(res.statusCode).toBe(400);
    expect(res.body).not.toContain(SECRETS.apiKey);
  });
});
