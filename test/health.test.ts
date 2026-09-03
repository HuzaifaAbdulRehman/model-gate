import type { Redis } from 'ioredis';
import type pg from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';
import { ProviderClient } from '../src/providers/client.js';
import { buildServer } from '../src/server.js';
import { createTestPool, createTestRedis, waitForRedis } from './helpers/db.js';
import { TEST_DATABASE_URL, TEST_REDIS_URL } from './helpers/global-setup.js';

const config = loadConfig({
  NODE_ENV: 'test',
  LOG_LEVEL: 'silent',
  DATABASE_URL: TEST_DATABASE_URL,
  REDIS_URL: TEST_REDIS_URL,
  // Long enough to satisfy the minimum and deliberately free of entropy, so a
  // secret scanner never has to decide whether this one is real.
  GATEWAY_API_KEY: 'x'.repeat(24),
});

const db: pg.Pool = createTestPool();
const cache: Redis = createTestRedis();
// Constructing one opens no sockets; pools are created on first use.
const client = new ProviderClient();

beforeAll(async () => {
  await waitForRedis(cache);
});

afterAll(async () => {
  await client.close();
  await cache.quit();
  await db.end();
});

describe('GET /health', () => {
  it('reports ok with an uptime', async () => {
    const app = buildServer({ config, db, cache, client });

    const res = await app.inject({ method: 'GET', url: '/health' });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ status: 'ok' });
    expect(res.json().uptime).toBeTypeOf('number');

    await app.close();
  });

  it('answers even when a dependency is unreachable', async () => {
    // Liveness must not follow readiness, or a broken Postgres gets the process
    // restarted repeatedly while Postgres is still the thing that is broken.
    const brokenDb = { query: () => Promise.reject(new Error('down')) } as unknown as pg.Pool;
    const app = buildServer({ config, db: brokenDb, cache, client });

    const res = await app.inject({ method: 'GET', url: '/health' });

    expect(res.statusCode).toBe(200);

    await app.close();
  });

  it('404s an unknown route', async () => {
    const app = buildServer({ config, db, cache, client });

    const res = await app.inject({ method: 'GET', url: '/nope' });

    expect(res.statusCode).toBe(404);

    await app.close();
  });
});

describe('GET /ready', () => {
  it('reports both dependencies reachable', async () => {
    const app = buildServer({ config, db, cache, client });

    const res = await app.inject({ method: 'GET', url: '/ready' });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ ready: true, postgres: true, redis: true });

    await app.close();
  });

  it('503s and names Postgres when the pool is unreachable', async () => {
    const brokenDb = { query: () => Promise.reject(new Error('down')) } as unknown as pg.Pool;
    const app = buildServer({ config, db: brokenDb, cache, client });

    const res = await app.inject({ method: 'GET', url: '/ready' });

    expect(res.statusCode).toBe(503);
    expect(res.json()).toEqual({ ready: false, postgres: false, redis: true });

    await app.close();
  });

  it('503s and names Redis when the cache is unreachable', async () => {
    const brokenCache = { ping: () => Promise.reject(new Error('down')) } as unknown as Redis;
    const app = buildServer({ config, db, cache: brokenCache, client });

    const res = await app.inject({ method: 'GET', url: '/ready' });

    expect(res.statusCode).toBe(503);
    expect(res.json()).toEqual({ ready: false, postgres: true, redis: false });

    await app.close();
  });
});
