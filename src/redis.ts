import { Redis } from 'ioredis';
import type { Config } from './config.js';

export type Cache = Redis;

export function createRedis(config: Config): Cache {
  return new Redis(config.REDIS_URL, {
    // Redis carries the token budget, so a command that cannot reach it has to
    // surface as an error the caller decides about. Queueing commands offline
    // would let requests through on a stale view of the budget and then apply
    // the reservations later, against a window that has already closed.
    enableOfflineQueue: false,
    maxRetriesPerRequest: 2,
    // Nothing here is worth waiting on for long: the limiter's whole value is
    // being cheaper than the provider call it guards.
    connectTimeout: 5_000,
  });
}
