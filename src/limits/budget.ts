import type { Cache } from '../redis.js';

/**
 * Token-aware rate limiting: reserve an estimate, then reconcile against what
 * was really used.
 *
 * Limiting by request count is the wrong unit, because one request can cost a
 * thousand times another. Limiting by tokens means the true cost is not known
 * until the response ends, so the worst case is held up front and the
 * difference given back afterwards.
 *
 * What this is correct about, and what it is not: each script runs atomically,
 * so concurrent admission overshoot is eliminated exactly. It is NOT correct
 * under crashes. A process that dies between reserve and settle has its whole
 * reservation reclaimed by the sweep, including the tokens the provider really
 * did generate, so the budget under-counts real consumption by that amount.
 * That is a deliberate trade of accuracy for liveness, bounded by one refill
 * period. Redis itself is not durable either: the default fsync loses about a
 * second. Per-script atomicity buys race-freedom, not crash-safety.
 */

/**
 * The reservation amount is encoded into the sorted-set member because Lua may
 * not construct key names. That single constraint decides the whole ledger
 * shape.
 */
const RESERVE_LUA = `
-- KEYS[1] bucket hash, KEYS[2] lease zset
-- ARGV: cap, ratePerSec, est, requestId, leaseTtlMs, pruneLimit
local cap        = tonumber(ARGV[1])
local rate       = tonumber(ARGV[2])
local est        = tonumber(ARGV[3])
local requestId  = ARGV[4]
local leaseTtl   = tonumber(ARGV[5])
local pruneLimit = tonumber(ARGV[6])

-- Before the prune, not after. Pruning first and then returning early would
-- accumulate the reclaimed tokens in a local and never write them back, so
-- every over-cap request would silently destroy whatever it had reclaimed.
if est > cap then
  return {-1, 0, 0}
end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

-- Reclaim expired leases, bounded. An unbounded loop here blocks every other
-- client, because Redis runs one script at a time.
local back = 0
local dead = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, pruneLimit)
if #dead > 0 then
  for i = 1, #dead do
    local amount = string.match(dead[i], '^(%d+):')
    if amount then back = back + tonumber(amount) end
  end
  redis.call('ZREM', KEYS[2], unpack(dead))
end

local bucket = redis.call('HMGET', KEYS[1], 'tk', 'ts')
local tk = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tk == nil then tk = cap; ts = now end

local elapsed = math.max(0, now - ts) / 1000
tk = math.min(cap, tk + elapsed * rate + back)

local admitted = 0
local retryAfter = 0
if tk >= est then
  tk = tk - est
  redis.call('ZADD', KEYS[2], now + leaseTtl, est .. ':' .. requestId)
  admitted = 1
else
  retryAfter = math.ceil((est - tk) / rate * 1000)
end

-- Formatted, never tostring: tostring can render a small float in scientific
-- notation, which tonumber on the next pass reads back as nil.
redis.call('HSET', KEYS[1], 'tk', string.format('%.4f', tk), 'ts', now)
redis.call('PEXPIRE', KEYS[1], leaseTtl * 4)
redis.call('PEXPIRE', KEYS[2], leaseTtl * 4)

return {admitted, math.floor(tk), retryAfter}
`;

const SETTLE_LUA = `
-- KEYS[1] bucket hash, KEYS[2] lease zset, KEYS[3] settled marker
-- ARGV: cap, ratePerSec, actual, requestId, est, markerTtlMs
local cap       = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local actual    = tonumber(ARGV[3])
local requestId = ARGV[4]
local est       = tonumber(ARGV[5])
local markerTtl = tonumber(ARGV[6])

-- Redis calls are at-least-once from the client's point of view: one that timed
-- out after the server applied it, and was then retried, would charge the
-- request twice under a scheme that only relied on ZREM. The marker makes a
-- repeat settle a no-op.
if redis.call('SET', KEYS[3], '1', 'NX', 'PX', markerTtl) == false then
  return {0, 0}
end

local outstanding = 0
if redis.call('ZREM', KEYS[2], est .. ':' .. requestId) == 1 then
  outstanding = est
end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local bucket = redis.call('HMGET', KEYS[1], 'tk', 'ts')
local tk = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tk == nil then tk = cap; ts = now end

local elapsed = math.max(0, now - ts) / 1000
tk = math.min(cap, tk + elapsed * rate)

-- Debt is allowed down to a full cap. Flooring at zero would silently forgive
-- an overage, which is the direction that lets a tenant exceed their budget for
-- free by always underestimating.
tk = math.max(-cap, math.min(cap, tk + outstanding - actual))

redis.call('HSET', KEYS[1], 'tk', string.format('%.4f', tk), 'ts', now)
redis.call('PEXPIRE', KEYS[1], markerTtl)

return {1, math.floor(tk)}
`;

export interface BudgetOptions {
  /** Bucket size, in tokens. Also the largest single request that can ever run. */
  capTokens: number;
  refillPerSec: number;
  /**
   * How long a reservation is held before the sweep reclaims it. Set well above
   * the provider body timeout, or a slow but healthy stream is reclaimed out
   * from under itself.
   */
  leaseTtlMs: number;
  /** Bounded so one call cannot block the server. */
  pruneLimit?: number;
  settledMarkerTtlMs?: number;
}

export interface ReserveResult {
  /** False when the tenant is out of budget for now. */
  admitted: boolean;
  /**
   * True when the request is larger than the bucket can ever hold. A different
   * answer from "not right now": no amount of waiting makes it fit.
   */
  impossible: boolean;
  remaining: number;
  retryAfterMs: number;
}

interface ScriptedCache extends Cache {
  mgReserve(
    bucket: string,
    lease: string,
    cap: string,
    rate: string,
    est: string,
    requestId: string,
    leaseTtl: string,
    pruneLimit: string,
  ): Promise<[number, number, number]>;
  mgSettle(
    bucket: string,
    lease: string,
    marker: string,
    cap: string,
    rate: string,
    actual: string,
    requestId: string,
    est: string,
    markerTtl: string,
  ): Promise<[number, number]>;
}

export class TokenBudget {
  readonly #cache: ScriptedCache;
  readonly #options: Required<BudgetOptions>;

  constructor(cache: Cache, options: BudgetOptions) {
    this.#options = {
      pruneLimit: 20,
      settledMarkerTtlMs: options.leaseTtlMs * 4,
      ...options,
    };

    // defineCommand rather than a hand-rolled EVALSHA: the script cache is lost
    // on restart and on replica promotion, and the client is what knows how to
    // fall back to EVAL when that happens.
    cache.defineCommand('mgReserve', { numberOfKeys: 2, lua: RESERVE_LUA });
    cache.defineCommand('mgSettle', { numberOfKeys: 3, lua: SETTLE_LUA });
    this.#cache = cache as ScriptedCache;
  }

  /**
   * One hash tag per tenant, so a single script never spans two cluster slots.
   */
  #keys(tenant: string, modelClass: string, requestId?: string) {
    const prefix = `mg:{t:${tenant}}`;
    return {
      bucket: `${prefix}:tb:${modelClass}`,
      lease: `${prefix}:lease:${modelClass}`,
      marker: `${prefix}:settled:${requestId ?? ''}`,
    };
  }

  async reserve(
    tenant: string,
    modelClass: string,
    requestId: string,
    estimate: number,
  ): Promise<ReserveResult> {
    // Enforced here rather than trusted from the caller. The Lua parses the
    // amount back out of the sorted-set member with a digits-only pattern, and
    // a fractional value would make that match fail, add nil to a number, and
    // abort the whole script. The limiter would fail hard instead of open.
    if (!Number.isInteger(estimate) || estimate < 0) {
      throw new TypeError(`token estimate must be a non-negative integer, got ${estimate}`);
    }

    const keys = this.#keys(tenant, modelClass);
    const [admitted, remaining, retryAfterMs] = await this.#cache.mgReserve(
      keys.bucket,
      keys.lease,
      String(this.#options.capTokens),
      String(this.#options.refillPerSec),
      String(estimate),
      requestId,
      String(this.#options.leaseTtlMs),
      String(this.#options.pruneLimit),
    );

    return {
      admitted: admitted === 1,
      impossible: admitted === -1,
      remaining,
      retryAfterMs,
    };
  }

  /**
   * Returns the unused part of the reservation. Safe to call more than once for
   * one request; only the first call has any effect.
   */
  async settle(
    tenant: string,
    modelClass: string,
    requestId: string,
    estimate: number,
    actual: number,
  ): Promise<{ settled: boolean; remaining: number }> {
    const keys = this.#keys(tenant, modelClass, requestId);
    const [settled, remaining] = await this.#cache.mgSettle(
      keys.bucket,
      keys.lease,
      keys.marker,
      String(this.#options.capTokens),
      String(this.#options.refillPerSec),
      String(Math.ceil(actual)),
      requestId,
      String(estimate),
      String(this.#options.settledMarkerTtlMs),
    );

    return { settled: settled === 1, remaining };
  }
}
