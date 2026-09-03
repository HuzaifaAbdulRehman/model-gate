import { setTimeout as delay } from 'node:timers/promises';
import type { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { TokenBudget } from '../src/limits/budget.js';
import { createTestRedis, waitForRedis } from './helpers/db.js';

const cache: Redis = createTestRedis();

const TENANT = 'acme';
const MODEL = 'mock-1';
const bucketKey = `mg:{t:${TENANT}}:tb:${MODEL}`;
const leaseKey = `mg:{t:${TENANT}}:lease:${MODEL}`;

function budget(overrides: Partial<ConstructorParameters<typeof TokenBudget>[1]> = {}) {
  return new TokenBudget(cache, {
    capTokens: 1_000,
    refillPerSec: 0,
    leaseTtlMs: 60_000,
    ...overrides,
  });
}

async function tokens(): Promise<number> {
  return Number(await cache.hget(bucketKey, 'tk'));
}

/** Rewrites a lease's expiry into the past, so the sweep sees it as abandoned. */
async function expireLease(estimate: number, requestId: string): Promise<void> {
  await cache.zadd(leaseKey, 1, `${estimate}:${requestId}`);
}

beforeAll(async () => {
  await waitForRedis(cache);
});

beforeEach(async () => {
  await cache.del(bucketKey, leaseKey);
  const settled = await cache.keys(`mg:{t:${TENANT}}:settled:*`);
  if (settled.length > 0) await cache.del(...settled);
});

afterAll(async () => {
  await cache.quit();
});

describe('reserving', () => {
  it('admits a request and holds its estimate', async () => {
    const b = budget();
    const result = await b.reserve(TENANT, MODEL, 'r1', 400);

    expect(result.admitted).toBe(true);
    expect(result.impossible).toBe(false);
    expect(result.remaining).toBe(600);
  });

  it('denies once the budget is gone and says when to come back', async () => {
    const b = budget({ refillPerSec: 100 });
    await b.reserve(TENANT, MODEL, 'r1', 900);

    const denied = await b.reserve(TENANT, MODEL, 'r2', 500);

    expect(denied.admitted).toBe(false);
    expect(denied.impossible).toBe(false);
    // 400 tokens short at 100 per second.
    expect(denied.retryAfterMs).toBeGreaterThanOrEqual(3_900);
    expect(denied.retryAfterMs).toBeLessThanOrEqual(4_100);
  });

  it('separates "never fits" from "not right now"', async () => {
    // A request bigger than the bucket can never be admitted, so telling the
    // caller to retry would be telling them to wait for something that will not
    // happen. The route turns this into a 400, not a 429.
    const b = budget();
    const result = await b.reserve(TENANT, MODEL, 'r1', 5_000);

    expect(result.impossible).toBe(true);
    expect(result.admitted).toBe(false);
  });

  it('refills over time', async () => {
    const b = budget({ refillPerSec: 2_000 });
    await b.reserve(TENANT, MODEL, 'r1', 1_000);
    expect(await tokens()).toBe(0);

    await delay(250);
    const after = await b.reserve(TENANT, MODEL, 'r2', 100);

    expect(after.admitted).toBe(true);
  });

  it('refuses a fractional estimate at the boundary', async () => {
    // The Lua parses the amount back out of the sorted-set member with a
    // digits-only pattern. A fractional value makes that match fail, adds nil
    // to a number, and aborts the script, so the limiter would fail hard rather
    // than open.
    const b = budget();

    await expect(b.reserve(TENANT, MODEL, 'r1', 12.5)).rejects.toThrow(/integer/);
    await expect(b.reserve(TENANT, MODEL, 'r1', -1)).rejects.toThrow(/integer/);
  });
});

describe('abandoned reservations', () => {
  it('reclaims a lease whose holder never came back', async () => {
    // A process that died between reserve and settle. Availability heals; the
    // accounting does not, because tokens the provider really generated are
    // handed back too. That is the deliberate trade.
    const b = budget();
    await b.reserve(TENANT, MODEL, 'dead', 900);
    expect(await tokens()).toBe(100);

    await expireLease(900, 'dead');
    const next = await b.reserve(TENANT, MODEL, 'r2', 900);

    expect(next.admitted).toBe(true);
  });

  it('does not destroy reclaimed tokens when a request is over cap', async () => {
    // The bug this ordering exists to avoid. Pruning first and then returning
    // early on the over-cap path would accumulate the reclaimed tokens in a
    // local, drop the leases, and never write the tokens back. Every over-cap
    // request would silently burn whatever it had just reclaimed.
    const b = budget();
    await b.reserve(TENANT, MODEL, 'a', 500);
    await b.reserve(TENANT, MODEL, 'b', 500);
    expect(await tokens()).toBe(0);

    await expireLease(500, 'a');
    await expireLease(500, 'b');

    const impossible = await b.reserve(TENANT, MODEL, 'huge', 5_000);
    expect(impossible.impossible).toBe(true);

    // The thousand tokens released by the two dead leases must still be there.
    const after = await b.reserve(TENANT, MODEL, 'c', 900);
    expect(after.admitted).toBe(true);
  });

  it('prunes at most the configured number of leases per call', async () => {
    // Redis runs one script at a time, so an unbounded sweep blocks every other
    // client on the server. The leases are written directly rather than through
    // reserve, because reserve prunes as it goes and would never leave more
    // than one abandoned lease to find.
    const b = budget({ pruneLimit: 2 });
    await b.reserve(TENANT, MODEL, 'spend', 1_000);
    expect(await tokens()).toBe(0);

    for (let i = 0; i < 5; i += 1) await expireLease(100, `dead${i}`);
    expect(await cache.zcard(leaseKey)).toBe(6);

    await b.reserve(TENANT, MODEL, 'sweep', 1);

    // Two of the five reclaimed, so two removed and one added for the sweep.
    expect(await cache.zcard(leaseKey)).toBe(5);
    // And only the two reclaimed amounts came back, not all five.
    expect(await tokens()).toBe(199);
  });
});

describe('settling', () => {
  it('gives back the difference between the estimate and the truth', async () => {
    const b = budget();
    await b.reserve(TENANT, MODEL, 'r1', 900);
    expect(await tokens()).toBe(100);

    const settled = await b.settle(TENANT, MODEL, 'r1', 900, 200);

    expect(settled.settled).toBe(true);
    expect(settled.remaining).toBe(800);
    expect(await cache.zcard(leaseKey)).toBe(0);
  });

  it('charges twice for one request only once', async () => {
    // A Redis call that timed out after the server applied it, then retried, is
    // an ordinary crash-adjacent case rather than a caller bug. Without the
    // marker the second settle would take the tokens again.
    const b = budget();
    await b.reserve(TENANT, MODEL, 'r1', 900);
    const first = await b.settle(TENANT, MODEL, 'r1', 900, 200);
    const second = await b.settle(TENANT, MODEL, 'r1', 900, 200);

    expect(first.settled).toBe(true);
    expect(second.settled).toBe(false);
    expect(await tokens()).toBe(800);
  });

  it('allows debt when the estimate was too low', async () => {
    // Flooring at zero would silently forgive the overage, which rewards
    // under-estimating.
    const b = budget();
    await b.reserve(TENANT, MODEL, 'r1', 900);
    const settled = await b.settle(TENANT, MODEL, 'r1', 900, 1_500);

    expect(settled.remaining).toBeLessThan(0);
  });

  it('is safe when the lease was already swept', async () => {
    // Nothing outstanding to give back, so the actual usage is simply charged.
    const b = budget();
    await b.reserve(TENANT, MODEL, 'r1', 900);
    await cache.del(leaseKey);

    const settled = await b.settle(TENANT, MODEL, 'r1', 900, 50);

    expect(settled.settled).toBe(true);
    expect(settled.remaining).toBe(50);
  });
});

describe('concurrency', () => {
  it('never admits more than the bucket holds', async () => {
    // The property a post-hoc counter cannot give you: twenty simultaneous
    // requests all read "there is room" before any of them writes.
    const b = budget();
    const results = await Promise.all(
      Array.from({ length: 20 }, (_, i) => b.reserve(TENANT, MODEL, `c${i}`, 100)),
    );

    expect(results.filter((r) => r.admitted).length).toBe(10);
    expect(await tokens()).toBe(0);
  });
});
