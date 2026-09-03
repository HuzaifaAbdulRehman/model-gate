import { setTimeout as delay } from 'node:timers/promises';
import type { FastifyInstance } from 'fastify';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';
import type { Db } from '../src/db.js';
import { ProviderClient } from '../src/providers/client.js';
import type { Cache } from '../src/redis.js';
import { buildServer } from '../src/server.js';
import { createTestRedis, waitForRedis } from './helpers/db.js';
import { startMock, type RunningMock } from './helpers/mock.js';

// Redis is real, because the token budget now sits in the request path and a
// stub would only prove that a stub was called. Postgres is still stubbed:
// audit writes land later in this phase.
const db = { query: () => Promise.resolve({ rows: [] }) } as unknown as Db;
const cache: Cache = createTestRedis();

const API_KEY = 'x'.repeat(24);
const TENANT = 'default';

interface Harness {
  app: FastifyInstance;
  primary: RunningMock;
  backup: RunningMock;
  close: () => Promise<void>;
}

async function harness(
  options: { primaryFails?: string; backupFails?: string; env?: Record<string, string> } = {},
): Promise<Harness> {
  const primary = await startMock(
    options.primaryFails !== undefined ? { alwaysFail: options.primaryFails } : {},
  );
  const backup = await startMock(
    options.backupFails !== undefined ? { alwaysFail: options.backupFails } : {},
  );

  const config = loadConfig({
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    DATABASE_URL: 'postgres://modelgate:modelgate@localhost:5433/modelgate',
    REDIS_URL: 'redis://localhost:6380',
    GATEWAY_API_KEY: API_KEY,
    MOCK_PRIMARY_URL: primary.url,
    MOCK_BACKUP_URL: backup.url,
    // Retries still happen; they just do not spend real time doing it.
    RETRY_BASE_MS: '0',
    RETRY_CAP_MS: '1',
    ...options.env,
  });

  const client = new ProviderClient();
  const app = buildServer({ config, db, cache, client });
  await app.ready();

  return {
    app,
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

let open: Harness | null = null;

beforeAll(async () => {
  await waitForRedis(cache);
});

afterEach(async () => {
  await open?.close();
  open = null;
  // Each test starts with a full budget and an empty cache, or a later test
  // fails for a reason that belongs to an earlier one. The cache keys are not
  // under the tenant hash tag, so they need their own sweep.
  const keys = [...(await cache.keys(`mg:{t:${TENANT}}:*`)), ...(await cache.keys('mg:cache:*'))];
  if (keys.length > 0) await cache.del(...keys);
});

afterAll(async () => {
  await cache.quit();
});

function chat(app: FastifyInstance, body: Record<string, unknown> = {}, key = API_KEY) {
  return app.inject({
    method: 'POST',
    url: '/v1/chat/completions',
    headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json' },
    payload: { model: 'mock-1', messages: [{ role: 'user', content: 'hello' }], ...body },
  });
}

describe('authentication', () => {
  it('rejects a request with no key', async () => {
    open = await harness();
    const res = await open.app.inject({
      method: 'POST',
      url: '/v1/chat/completions',
      payload: { model: 'mock-1', messages: [{ role: 'user', content: 'hi' }] },
    });

    expect(res.statusCode).toBe(401);
    expect(open.primary.count()).toBe(0);
  });

  it('rejects a wrong key and never calls a provider', async () => {
    open = await harness();
    const res = await chat(open.app, {}, 'y'.repeat(24));

    expect(res.statusCode).toBe(401);
    expect(open.primary.count()).toBe(0);
  });

  it('rejects a key that is merely a prefix of the real one', async () => {
    // A startsWith check would accept this, and so would any comparison that
    // stops at the first difference.
    open = await harness();
    const res = await chat(open.app, {}, 'x'.repeat(20));

    expect(res.statusCode).toBe(401);
  });

  it('leaves the health probes unauthenticated', async () => {
    // They answer a load balancer that has no credential to offer.
    open = await harness();

    expect((await open.app.inject({ method: 'GET', url: '/health' })).statusCode).toBe(200);
  });
});

describe('proxying', () => {
  it('returns the provider body and names who served it', async () => {
    open = await harness();
    const res = await chat(open.app);

    expect(res.statusCode).toBe(200);
    expect(res.headers['x-modelgate-provider']).toBe('mock-primary');
    expect(res.headers['x-modelgate-attempts']).toBe('1');

    const body = res.json() as { object: string; choices: Array<{ message: { content: string } }> };
    expect(body.object).toBe('chat.completion');
    expect(body.choices[0]?.message.content.length).toBeGreaterThan(0);

    expect(open.primary.count()).toBe(1);
    expect(open.backup.count()).toBe(0);
  });

  it('rejects a body with no messages before calling anyone', async () => {
    open = await harness();
    const res = await open.app.inject({
      method: 'POST',
      url: '/v1/chat/completions',
      headers: { authorization: `Bearer ${API_KEY}` },
      payload: { model: 'mock-1', messages: [] },
    });

    expect(res.statusCode).toBe(400);
    expect(open.primary.count()).toBe(0);
  });

  it('says streaming is not implemented rather than silently not streaming', async () => {
    open = await harness();
    const res = await chat(open.app, { stream: true });

    expect(res.statusCode).toBe(501);
    expect(open.primary.count()).toBe(0);
  });
});

describe('failover', () => {
  it('moves to the backup when the primary is rate limited', async () => {
    // A 429 means this provider has no quota now. Waiting for its window to
    // reopen is strictly worse than asking one that has quota.
    open = await harness({ primaryFails: '429' });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(200);
    expect(res.headers['x-modelgate-provider']).toBe('mock-backup');
    // One attempt each: a 429 does not get retried on the same provider.
    expect(open.primary.count()).toBe(1);
    expect(open.backup.count()).toBe(1);
  });

  it('retries the same provider on a 500 before giving up on it', async () => {
    open = await harness({ primaryFails: 'server-error' });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(200);
    expect(res.headers['x-modelgate-provider']).toBe('mock-backup');
    // Two tries at the primary, matching MAX_ATTEMPTS_PER_PROVIDER, then one
    // at the backup.
    expect(open.primary.count()).toBe(2);
    expect(open.backup.count()).toBe(1);
    expect(res.headers['x-modelgate-attempts']).toBe('3');
  });

  it('moves on when the primary drops the connection', async () => {
    open = await harness({ primaryFails: 'abort@0' });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(200);
    expect(res.headers['x-modelgate-provider']).toBe('mock-backup');
  });

  it('does NOT fail over on a 400', async () => {
    // The single most important test in this file. A malformed request is
    // rejected identically by every provider, so failing over turns one clean
    // 400 into a call against each one and an error naming the wrong provider.
    open = await harness({ primaryFails: 'bad-request' });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(400);
    expect(open.primary.count()).toBe(1);
    expect(open.backup.count()).toBe(0);
    expect(res.headers['x-modelgate-attempts']).toBe('1');

    const body = res.json() as { error: { type: string; message: string } };
    expect(body.error.type).toBe('invalid_request_error');
    // The provider's own explanation survives, rather than being flattened into
    // a generic gateway message.
    expect(body.error.message).toMatch(/unsupported value for parameter model/);
  });

  it('returns 502 once every provider has failed', async () => {
    open = await harness({ primaryFails: '429', backupFails: '429' });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(502);
    const body = res.json() as { error: { type: string } };
    expect(body.error.type).toBe('upstream_error');
    expect(open.primary.count()).toBe(1);
    expect(open.backup.count()).toBe(1);
  });

  it('reports 502 rather than the last provider status', async () => {
    // Every provider was tried and none could serve it. That is a gateway
    // condition, not the 500 the last one happened to return.
    open = await harness({ primaryFails: 'server-error', backupFails: 'server-error' });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(502);
    expect(open.primary.count()).toBe(2);
    expect(open.backup.count()).toBe(2);
  });

  it('gives up on a provider that never answers', async () => {
    // The mock sends no headers at all, so only the header timeout ends it.
    // Without one, undici waits five minutes by default, which a caller cannot
    // tell apart from a hang.
    open = await harness({
      primaryFails: 'hang',
      env: { PROVIDER_HEADERS_TIMEOUT_MS: '200' },
    });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(200);
    expect(res.headers['x-modelgate-provider']).toBe('mock-backup');
  });

  it('honours a reordered provider chain', async () => {
    open = await harness({
      primaryFails: '429',
      env: { PROVIDER_ORDER: 'mock-backup,mock-primary' },
    });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(200);
    expect(res.headers['x-modelgate-provider']).toBe('mock-backup');
    // The primary is never reached, because the backup answered first.
    expect(open.primary.count()).toBe(0);
  });
});

describe('caching', () => {
  it('serves a repeat request without calling a provider', async () => {
    open = await harness();
    const first = await chat(open.app);
    const second = await chat(open.app);

    expect(first.headers['x-modelgate-cache']).toBe('miss');
    expect(second.headers['x-modelgate-cache']).toBe('hit');
    expect(second.json()).toEqual(first.json());
    expect(open.primary.count()).toBe(1);
  });

  it('costs no tokens on a hit', async () => {
    // A hit did no upstream work, so charging the tenant's budget for it would
    // be charging them for the gateway's own memory.
    open = await harness();
    await chat(open.app);
    const before = await cache.hget(`mg:{t:${TENANT}}:tb:all`, 'tk');
    await chat(open.app);
    const after = await cache.hget(`mg:{t:${TENANT}}:tb:all`, 'tk');

    expect(after).toBe(before);
  });

  it('treats a different prompt as a different question', async () => {
    open = await harness();
    await chat(open.app);
    const other = await chat(open.app, { messages: [{ role: 'user', content: 'different' }] });

    expect(other.headers['x-modelgate-cache']).toBe('miss');
    expect(open.primary.count()).toBe(2);
  });

  it('ignores the order fields were written in', async () => {
    open = await harness();
    await chat(open.app, { temperature: 0.5 });
    const reordered = await open.app.inject({
      method: 'POST',
      url: '/v1/chat/completions',
      headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
      payload: {
        temperature: 0.5,
        messages: [{ role: 'user', content: 'hello' }],
        model: 'mock-1',
      },
    });

    expect(reordered.headers['x-modelgate-cache']).toBe('hit');
  });

  it('does not store a failed response', async () => {
    open = await harness({ primaryFails: '429', backupFails: '429' });
    expect((await chat(open.app)).statusCode).toBe(502);
    const retry = await chat(open.app);

    expect(retry.statusCode).toBe(502);
    expect(retry.headers['x-modelgate-cache']).toBeUndefined();
  });

  it('can be turned off', async () => {
    open = await harness({ env: { CACHE_ENABLED: 'false' } });
    await chat(open.app);
    const second = await chat(open.app);

    expect(second.headers['x-modelgate-cache']).toBe('miss');
    expect(open.primary.count()).toBe(2);
  });
});

describe('token budget', () => {
  it('refuses an impossible request with 400 rather than 429', async () => {
    // 429 would tell the caller to retry something that can never fit, however
    // long they wait.
    open = await harness({ env: { TOKEN_BUDGET_CAP: '100' } });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(400);
    expect((res.json() as { error: { code: string } }).error.code).toBe('request_exceeds_budget');
    expect(open.primary.count()).toBe(0);
  });

  it('holds a reservation so concurrent requests cannot all pass', async () => {
    // The property a post-hoc counter cannot give you. All three read the
    // budget before any of them has finished spending it, so without a
    // reservation all three would be admitted.
    open = await harness({
      env: { TOKEN_BUDGET_CAP: '2000', TOKEN_REFILL_PER_SEC: '1' },
    });
    const results = await Promise.all([chat(open.app), chat(open.app), chat(open.app)]);

    expect(results.filter((r) => r.statusCode === 200).length).toBe(1);
    const limited = results.filter((r) => r.statusCode === 429);
    expect(limited.length).toBe(2);
    expect(limited[0]?.headers['retry-after']).toBeDefined();
    expect((limited[0]?.json() as { error: { code: string } }).error.code).toBe(
      'token_budget_exhausted',
    );
  });

  it('gives back the part of the reservation nobody used', async () => {
    // The reservation covers max_tokens as a worst case, and a short answer
    // must not go on costing the tenant the difference.
    open = await harness();
    const res = await chat(open.app);

    expect(res.statusCode).toBe(200);
    const remaining = Number(res.headers['x-modelgate-tokens-remaining']);
    expect(remaining).toBeGreaterThan(99_000);
    expect(remaining).toBeLessThan(100_000);
  });

  it('says whether the token count came from the provider or an estimate', async () => {
    open = await harness();
    const res = await chat(open.app);

    expect(res.headers['x-modelgate-token-source']).toBe('provider');
  });

  it('returns the whole reservation when nothing was served', async () => {
    open = await harness({ primaryFails: '429', backupFails: '429' });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(502);
    expect(Number(res.headers['x-modelgate-tokens-remaining'])).toBe(100_000);
  });
});

describe('the chain deadline', () => {
  it('stops rather than working through every provider', async () => {
    // Each call is bounded on its own, but the worst case is providers times
    // attempts times timeout. A caller should not wait all of that for a 502
    // that became inevitable at the first provider.
    open = await harness({
      primaryFails: 'server-error',
      env: { REQUEST_DEADLINE_MS: '1' },
    });
    const res = await chat(open.app);

    expect(res.statusCode).toBe(502);
    expect(res.headers['x-modelgate-attempts']).toBe('1');
    expect(open.backup.count()).toBe(0);
  });
});

describe('client disconnect', () => {
  it('stops working through the chain once the caller hangs up', async () => {
    // Needs a real socket: app.inject has no connection to drop.
    open = await harness({
      primaryFails: 'hang',
      env: { PROVIDER_HEADERS_TIMEOUT_MS: '200' },
    });
    await open.app.listen({ port: 0, host: '127.0.0.1' });
    const address = open.app.server.address();
    if (address === null || typeof address === 'string') throw new Error('no port');

    const controller = new AbortController();
    const inFlight = fetch(`http://127.0.0.1:${address.port}/v1/chat/completions`, {
      method: 'POST',
      headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'mock-1', messages: [{ role: 'user', content: 'hi' }] }),
      signal: controller.signal,
    }).catch((err: unknown) => err);

    await delay(100);
    controller.abort();
    await inFlight;

    // Measured, not guessed: with the disconnect ignored, the primary is
    // retried at ~1.4s and the backup reached at ~2.2s. Undici's header timeout
    // is coarse, so a 200ms setting really fires at about a second, and a
    // shorter wait here would pass without proving anything.
    await delay(2_600);
    expect(open.primary.count()).toBe(1);
    expect(open.backup.count()).toBe(0);
  }, 15_000);
});
