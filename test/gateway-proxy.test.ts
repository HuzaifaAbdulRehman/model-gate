import { setTimeout as delay } from 'node:timers/promises';
import type { FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';
import type { Db } from '../src/db.js';
import { ProviderClient } from '../src/providers/client.js';
import type { Cache } from '../src/redis.js';
import { buildServer } from '../src/server.js';
import { startMock, type RunningMock } from './helpers/mock.js';

// The chat route does not touch Postgres or Redis yet; audit writes and the
// token budget arrive in phase 2b. Stubbing them keeps these tests about
// routing and failover, and they will be swapped for the real handles when
// there is something to assert about them.
const db = { query: () => Promise.resolve({ rows: [] }) } as unknown as Db;
const cache = { ping: () => Promise.resolve('PONG') } as unknown as Cache;

const API_KEY = 'x'.repeat(24);

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

afterEach(async () => {
  await open?.close();
  open = null;
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
