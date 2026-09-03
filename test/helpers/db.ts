import { Redis } from 'ioredis';
import pg from 'pg';
import { TEST_DATABASE_URL, TEST_REDIS_URL } from './global-setup.js';

export function createTestPool(): pg.Pool {
  return new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 5 });
}

export function createTestRedis(): Redis {
  return new Redis(TEST_REDIS_URL, { enableOfflineQueue: false, maxRetriesPerRequest: 2 });
}

/**
 * ioredis connects in the background, and `enableOfflineQueue: false` means a
 * command issued before the socket is up is rejected rather than queued. That
 * is the behaviour the gateway wants, so a test that needs a live connection has
 * to wait for it rather than assume the constructor was enough.
 */
export function waitForRedis(cache: Redis): Promise<void> {
  if (cache.status === 'ready') return Promise.resolve();
  return new Promise((resolve, reject) => {
    cache.once('ready', () => resolve());
    cache.once('error', reject);
  });
}

/** Cheaper than re-running migrations between files. */
export async function truncateAll(db: pg.Pool): Promise<void> {
  await db.query('TRUNCATE requests, request_payloads, request_attempts, idempotency_keys');
}
