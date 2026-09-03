import { createHash } from 'node:crypto';
import type { ChatRequest } from '../providers/profile.js';
import type { Cache } from '../redis.js';

/**
 * Bumped whenever the key recipe or the stored shape changes.
 *
 * A shared Redis outlives every deploy, so without a version in the key the new
 * code reads a payload the old code wrote and deserializes it as though the
 * shape had not moved.
 */
const KEY_VERSION = 'v1';

/**
 * Recursively sorts object keys so two requests that differ only in field order
 * produce one key. Array order is preserved, because the order of messages is
 * part of the question being asked.
 */
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (typeof value === 'object' && value !== null) {
    const source = value as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(source).sort()) out[key] = canonical(source[key]);
    return out;
  }
  return value;
}

export interface CacheOptions {
  enabled: boolean;
  ttlSeconds: number;
}

export interface CacheLookup {
  hit: boolean;
  body: Record<string, unknown> | null;
}

export class CompletionCache {
  readonly #cache: Cache;
  readonly #options: CacheOptions;

  constructor(cache: Cache, options: CacheOptions) {
    this.#cache = cache;
    this.#options = options;
  }

  get enabled(): boolean {
    return this.#options.enabled;
  }

  /**
   * Keyed on the RAW request, deliberately, and this must not be "improved"
   * later by keying on the redacted one.
   *
   * Redaction replaces a value with a placeholder plus a stable id. Two prompts
   * that differ only in the address they mention would redact to the same text
   * and therefore to the same key, and the second caller would be served the
   * first caller's completion. It is a correctness bug dressed as a privacy
   * feature, and it crosses tenants.
   */
  key(request: ChatRequest, tenant: string): string {
    const digest = createHash('sha256')
      .update(JSON.stringify(canonical(request)))
      .digest('hex');
    // Scoped by tenant as well as by content. Sharing an entry between tenants
    // would leak one caller's prompt through another's completion.
    return `mg:cache:${KEY_VERSION}:${tenant}:${digest}`;
  }

  async get(key: string): Promise<CacheLookup> {
    if (!this.#options.enabled) return { hit: false, body: null };

    const raw = await this.#cache.get(key);
    if (raw === null) return { hit: false, body: null };

    try {
      return { hit: true, body: JSON.parse(raw) as Record<string, unknown> };
    } catch {
      // A stored value the current code cannot read is a stale shape, not an
      // outage. Treating it as a miss costs one provider call; throwing would
      // fail the request for a caching detail.
      return { hit: false, body: null };
    }
  }

  async set(key: string, body: Record<string, unknown>): Promise<void> {
    if (!this.#options.enabled) return;
    await this.#cache.set(key, JSON.stringify(body), 'EX', this.#options.ttlSeconds);
  }
}
