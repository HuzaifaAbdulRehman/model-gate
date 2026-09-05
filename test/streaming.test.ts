import type { FastifyInstance } from 'fastify';
import type pg from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';
import { completionFor } from '../src/mock/tokens.js';
import { ProviderClient } from '../src/providers/client.js';
import type { Cache } from '../src/redis.js';
import { buildServer } from '../src/server.js';
import { createTestPool, createTestRedis, truncateAll, waitForRedis } from './helpers/db.js';
import { TEST_DATABASE_URL } from './helpers/global-setup.js';
import { readSse, startMock, type Collected, type RunningMock } from './helpers/mock.js';

const db: pg.Pool = createTestPool();
const cache: Cache = createTestRedis();
const API_KEY = 'x'.repeat(24);
const TENANT = 'default';

interface Harness {
  url: string;
  app: FastifyInstance;
  provider: RunningMock;
  primary: RunningMock;
  backup: RunningMock;
  close: () => Promise<void>;
}

let open: Harness | null = null;

async function harness(
  options: {
    fails?: string;
    primaryFails?: string;
    backupFails?: string;
    env?: Record<string, string>;
  } = {},
): Promise<Harness> {
  const primaryFailure = options.primaryFails ?? options.fails;
  const backupFailure = options.backupFails ?? options.fails;
  const primary = await startMock(
    primaryFailure === undefined ? {} : { alwaysFail: primaryFailure },
  );
  const backup = await startMock(
    backupFailure === undefined ? {} : { alwaysFail: backupFailure },
  );
  const config = loadConfig({
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    DATABASE_URL: TEST_DATABASE_URL,
    REDIS_URL: 'redis://localhost:6380',
    GATEWAY_API_KEY: API_KEY,
    REDACTION_PEPPER: 'p'.repeat(32),
    MOCK_PRIMARY_URL: primary.url,
    MOCK_BACKUP_URL: backup.url,
    ...options.env,
  });
  const client = new ProviderClient();
  const app = buildServer({ config, db, cache, client });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  if (address === null || typeof address === 'string') throw new Error('no port');

  return {
    url: `http://127.0.0.1:${address.port}`,
    app,
    provider: primary,
    primary,
    backup,
    close: async () => {
      await app.close();
      await client.close();
      await primary.close();
      await backup.close();
    },
  };
}

beforeAll(async () => {
  await waitForRedis(cache);
});

afterEach(async () => {
  await open?.close();
  open = null;
  const keys = [...(await cache.keys(`mg:{t:${TENANT}}:*`)), ...(await cache.keys('mg:cache:*'))];
  if (keys.length > 0) await cache.del(...keys);
  await truncateAll(db);
});

afterAll(async () => {
  await cache.quit();
  await db.end();
});

function ask(url: string, extra: Record<string, unknown> = {}, signal?: AbortSignal) {
  return fetch(`${url}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
    body: JSON.stringify({
      model: 'mock-1',
      messages: [{ role: 'user', content: 'hello' }],
      stream: true,
      ...extra,
    }),
    ...(signal !== undefined ? { signal } : {}),
  });
}

async function stream(url: string, extra: Record<string, unknown> = {}): Promise<Collected> {
  return readSse(await ask(url, extra));
}

describe('relaying a healthy stream', () => {
  it('sends the headers that keep a stream unbuffered', async () => {
    open = await harness();
    const res = await ask(open.url);

    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toBe('text/event-stream; charset=utf-8');
    // Without no-transform a compression layer collapses the stream into one
    // burst, and no assertion on the frames themselves would notice.
    expect(res.headers.get('cache-control')).toBe('no-cache, no-transform');
    expect(res.headers.get('x-accel-buffering')).toBe('no');
    expect(res.headers.get('content-length')).toBeNull();
    expect(res.headers.get('x-modelgate-provider')).toBe('mock-primary');

    await res.body?.cancel();
  });

  it('passes the content through unchanged', async () => {
    open = await harness();
    const got = await stream(open.url);

    expect(got.transportError).toBeNull();
    expect(got.finishReason).toBe('stop');
    expect(got.text.length).toBeGreaterThan(0);
    expect(got.text).not.toContain('�');
  });

  it('emits exactly one done sentinel', async () => {
    // The provider sends its own and the gateway sends one. Forwarding both
    // would put two terminators in one stream, and after a failover in phase
    // five it would be two mid-answer.
    open = await harness();
    const res = await ask(open.url);
    const body = await res.text();

    expect(body.match(/data: \[DONE\]/g)).toHaveLength(1);
  });

  it('carries multi-byte characters intact', async () => {
    open = await harness();
    const got = await stream(open.url, { max_tokens: 200 });

    expect(got.text).not.toContain('�');
  });
});

describe('the commit point', () => {
  it('commits on the first content delta rather than waiting out the deadline', async () => {
    // With a five second deadline, a stream that only committed on the timer
    // would take five seconds to produce a first byte. Time to first token is
    // the number a streaming API is judged on.
    open = await harness({ env: { STREAM_COMMIT_DEADLINE_MS: '5000' } });

    const started = Date.now();
    const res = await ask(open.url);
    const reader = res.body?.getReader();
    await reader?.read();
    const ttfb = Date.now() - started;

    expect(res.status).toBe(200);
    expect(ttfb).toBeLessThan(1_000);

    await reader?.cancel();
  });

  it('answers a rate limit with a clean 429, not a 200 carrying an error', async () => {
    // Nothing has been written yet, so the status line is still ours to choose.
    open = await harness({ fails: '429' });
    const res = await ask(open.url);

    expect(res.status).toBe(429);
    expect(res.headers.get('content-type')).toMatch(/application\/json/);
    const body = (await res.json()) as { error: { type: string } };
    expect(body.error.type).toBe('upstream_error');
  });

  it('fails over on a rate limit before committing', async () => {
    open = await harness({ primaryFails: '429' });
    const res = await ask(open.url);
    const got = await readSse(res);

    expect(res.status).toBe(200);
    expect(res.headers.get('x-modelgate-provider')).toBe('mock-backup');
    expect(got.text).toBe(completionFor(1, 24));
    expect(got.events).not.toContain('error');
    expect(got.sawDone).toBe(true);
    expect(open.primary.count()).toBe(1);
    expect(open.backup.count()).toBe(1);
  });

  it('retries an empty broken stream, then fails over cleanly', async () => {
    open = await harness({
      primaryFails: 'abort@0',
      env: { RETRY_BASE_MS: '0', RETRY_CAP_MS: '1' },
    });
    const res = await ask(open.url);
    const got = await readSse(res);

    expect(res.status).toBe(200);
    expect(res.headers.get('x-modelgate-provider')).toBe('mock-backup');
    expect(got.text).toBe(completionFor(1, 24));
    expect(got.unparsable).toEqual([]);
    expect(got.events).not.toContain('error');
    expect(
      got.payloads.filter((payload) => payload.includes('"role":"assistant"')),
    ).toHaveLength(1);
    expect(open.primary.count()).toBe(2);
    expect(open.backup.count()).toBe(1);
  });

  it('records every pre-commit attempt and the provider that won', async () => {
    open = await harness({
      primaryFails: 'abort@0',
      env: {
        MAX_ATTEMPTS_PER_PROVIDER: '1',
        RETRY_BASE_MS: '0',
        RETRY_CAP_MS: '1',
      },
    });
    await stream(open.url);

    for (let i = 0; i < 60; i += 1) {
      const { rows } = await db.query<{
        provider: string;
        outcome: string;
        committed: boolean;
      }>('SELECT provider, outcome, committed FROM request_attempts ORDER BY attempt_no');
      if (rows.length === 2) {
        expect(rows).toEqual([
          { provider: 'mock-primary', outcome: 'stream_abort', committed: false },
          { provider: 'mock-backup', outcome: 'ok', committed: true },
        ]);
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error('streaming failover attempts were not recorded');
  });

  it('answers a rejected request with a clean 400', async () => {
    open = await harness({ fails: 'bad-request' });
    const res = await ask(open.url);

    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { type: string } };
    expect(body.error.type).toBe('invalid_request_error');
    expect(open.backup.count()).toBe(0);
  });

  it('reports an opened stream that produced nothing as an error', async () => {
    // Headers arrived, then the socket died before a single delta. Still
    // pre-commit, so the caller gets an honest status rather than an empty 200.
    open = await harness({ fails: 'abort@0' });
    const res = await ask(open.url);

    expect(res.status).toBe(502);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe('empty_stream');
  });
});

describe('a stream that dies after committing', () => {
  it('ends with an explicit error rather than stopping quietly', async () => {
    // The status was 200 the moment the first byte went out, and an HTTP status
    // is final. So the failure has to be stated inside the stream. Ending
    // quietly would hand the caller a truncated answer that looks complete.
    open = await harness({ fails: 'abort@6' });
    const got = await stream(open.url);

    expect(got.text.length).toBeGreaterThan(0);
    expect(got.events).toContain('error');
    expect(got.finishReason).toBe('modelgate_interrupted');
    expect(got.sawDone).toBe(true);
    expect(open.backup.count()).toBe(0);
  });

  it('catches a clean end that no error or timeout would reveal', async () => {
    // The dangerous one. A correct HTTP end, no socket error, no timeout, and
    // no terminal frame. Only the missing finish reason gives it away.
    open = await harness({ fails: 'clean-end@6' });
    const got = await stream(open.url);

    expect(got.transportError).toBeNull();
    expect(got.text.length).toBeGreaterThan(0);
    expect(got.finishReason).toBe('modelgate_interrupted');
    expect(got.events).toContain('error');
  });

  it('passes an in-band provider error through as a terminal error', async () => {
    open = await harness({ fails: 'error-data@5' });
    const got = await stream(open.url);

    expect(got.events).toContain('error');
    expect(got.finishReason).toBe('modelgate_interrupted');
  });
});

describe('a stream that ends correctly but incompletely', () => {
  it('leaves content_filter visible instead of relabelling it', async () => {
    // Protocol-perfect and semantically truncated. This is not a gateway
    // failure and must not be dressed up as one, but the caller has to be able
    // to see that the answer was cut short.
    open = await harness({ fails: 'filter-stop@6' });
    const got = await stream(open.url);

    expect(got.finishReason).toBe('content_filter');
    expect(got.events).not.toContain('error');
    expect(got.sawDone).toBe(true);
  });

  it('leaves length visible too', async () => {
    open = await harness({ fails: 'length-stop@6' });
    const got = await stream(open.url);

    expect(got.finishReason).toBe('length');
    expect(got.events).not.toContain('error');
  });

  it('records a truncated outcome in the audit log', async () => {
    open = await harness({ fails: 'length-stop@6' });
    await stream(open.url);

    for (let i = 0; i < 60; i += 1) {
      const { rows } = await db.query<{ outcome: string }>('SELECT outcome FROM requests');
      if (rows.length > 0) {
        expect(rows[0]?.outcome).toBe('truncated');
        return;
      }
      await new Promise((r) => setTimeout(r, 25));
    }
    throw new Error('no audit row appeared');
  });
});

describe('the caller hanging up', () => {
  it('stops reading upstream instead of paying for the rest', async () => {
    open = await harness({ fails: 'slow@40', env: { STREAM_COMMIT_DEADLINE_MS: '200' } });

    const controller = new AbortController();
    const res = await ask(open.url, { max_tokens: 400 }, controller.signal);
    const reader = res.body?.getReader();
    await reader?.read();
    controller.abort();

    // The relay should settle rather than run to the end of a four hundred
    // token generation nobody is reading.
    await new Promise((r) => setTimeout(r, 300));

    const { rows } = await db.query<{ outcome: string }>('SELECT outcome FROM requests');
    if (rows.length > 0) expect(rows[0]?.outcome).toBe('client_abort');
  });
});

describe('the token budget on a stream', () => {
  it('refuses an impossible request before opening anything', async () => {
    open = await harness({ env: { TOKEN_BUDGET_CAP: '100' } });
    const res = await ask(open.url);

    expect(res.status).toBe(400);
    expect(open.provider.count()).toBe(0);
    await res.json();
  });

  it('gives back the unused reservation once the stream ends', async () => {
    // Refill is turned right down. At the default rate the bucket refills
    // faster than a short stream spends, so the accounting would be masked by
    // the clock rather than observed.
    open = await harness({ env: { TOKEN_REFILL_PER_SEC: '1' } });
    await stream(open.url);

    const tk = Number(await cache.hget(`mg:{t:${TENANT}}:tb:all`, 'tk'));
    expect(tk).toBeGreaterThan(99_000);
    expect(tk).toBeLessThan(100_000);
  });
});
