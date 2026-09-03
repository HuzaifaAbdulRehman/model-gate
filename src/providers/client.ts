import { Pool } from 'undici';
import {
  classifyError,
  classifyStatus,
  errorCode,
  type AttemptFailure,
} from './errors.js';
import { shapeRequest, type ChatRequest, type ProviderProfile, type Usage } from './profile.js';

export interface AttemptSuccess {
  ok: true;
  provider: string;
  status: number;
  body: Record<string, unknown>;
  usage: Usage | null;
  latencyMs: number;
  providerRequestId: string | null;
}

export interface AttemptError extends AttemptFailure {
  ok: false;
  provider: string;
  latencyMs: number;
}

export type AttemptResult = AttemptSuccess | AttemptError;

export interface CallOptions {
  apiKey?: string | undefined;
  signal?: AbortSignal | undefined;
  now?: () => number;
}

/**
 * Owns one connection pool per provider origin.
 *
 * A pool rather than a fresh connection per request because an SSE response
 * occupies its socket for the whole of its life, so connection reuse is the
 * difference between a handful of sockets and one per in-flight stream.
 */
export class ProviderClient {
  readonly #pools = new Map<string, Pool>();

  #pool(origin: string): Pool {
    let pool = this.#pools.get(origin);
    if (pool === undefined) {
      pool = new Pool(origin, { connections: 64, pipelining: 0 });
      this.#pools.set(origin, pool);
    }
    return pool;
  }

  async call(
    profile: ProviderProfile,
    request: ChatRequest,
    options: CallOptions = {},
  ): Promise<AttemptResult> {
    const now = options.now ?? Date.now;
    const started = now();

    const shaped = shapeRequest(profile, request);
    if ('reject' in shaped) {
      // Refused before dispatch. Every provider would give the same answer, so
      // sending it anyway would just collect N copies of one 400.
      return {
        ok: false,
        provider: profile.name,
        disposition: 'fatal',
        status: 400,
        code: 'unsupported_combination',
        message: shaped.reject,
        retryAfterMs: null,
        latencyMs: 0,
      };
    }

    try {
      const response = await this.#pool(profile.origin).request({
        path: profile.path,
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          ...profile.authHeader(options.apiKey),
        },
        body: JSON.stringify(shaped.body),
        headersTimeout: profile.timeouts.headers,
        // The inter-chunk idle timer, not a total deadline. Undici's default is
        // five minutes, which is indistinguishable from hanging forever.
        bodyTimeout: profile.timeouts.body,
        ...(options.signal !== undefined ? { signal: options.signal } : {}),
      });

      const limits = profile.parseRateLimitHeaders(response.headers);

      if (response.statusCode >= 400) {
        const text = await response.body.text();
        return {
          ok: false,
          provider: profile.name,
          disposition: classifyStatus(response.statusCode, profile),
          status: response.statusCode,
          code: providerErrorCode(text) ?? `http_${response.statusCode}`,
          message: providerErrorMessage(text) ?? text.slice(0, 200),
          retryAfterMs: limits.resetMs,
          latencyMs: now() - started,
        };
      }

      const body = (await response.body.json()) as Record<string, unknown>;
      return {
        ok: true,
        provider: profile.name,
        status: response.statusCode,
        body,
        usage: profile.extractUsage(body),
        latencyMs: now() - started,
        providerRequestId: providerRequestId(body),
      };
    } catch (err) {
      return {
        ok: false,
        provider: profile.name,
        disposition: classifyError(err),
        status: null,
        code: errorCode(err),
        message: (err as Error).message ?? 'provider call failed',
        retryAfterMs: null,
        latencyMs: now() - started,
      };
    }
  }

  async close(): Promise<void> {
    await Promise.all([...this.#pools.values()].map((pool) => pool.close()));
    this.#pools.clear();
  }
}

function parseErrorBody(text: string): { error?: { message?: string; code?: string } } | null {
  try {
    return JSON.parse(text) as { error?: { message?: string; code?: string } };
  } catch {
    // Providers return HTML from their edge often enough that a parse failure
    // is an ordinary case, not an exceptional one.
    return null;
  }
}

function providerErrorMessage(text: string): string | null {
  return parseErrorBody(text)?.error?.message ?? null;
}

function providerErrorCode(text: string): string | null {
  return parseErrorBody(text)?.error?.code ?? null;
}

/** Nullable by nature: not every provider returns one, and not on every path. */
function providerRequestId(body: Record<string, unknown>): string | null {
  const groq = body['x_groq'];
  if (typeof groq === 'object' && groq !== null && typeof (groq as { id?: unknown }).id === 'string') {
    return (groq as { id: string }).id;
  }
  return typeof body['id'] === 'string' ? (body['id'] as string) : null;
}
