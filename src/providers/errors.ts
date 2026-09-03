import type { ProviderProfile } from './profile.js';

/**
 * What to do about a failed attempt.
 *
 * Getting this wrong is the classic gateway bug in both directions. Failing
 * over on a malformed request sends the same bad body to every provider in
 * turn, turning one clean 400 into N calls and a confusing error. Treating a
 * transient 503 as fatal throws away the reason the gateway exists.
 */
export type Disposition =
  /** The request itself is wrong. No provider will accept it. Stop. */
  | 'fatal'
  /** Likely to clear on its own. Wait, then ask the same provider again. */
  | 'retry'
  /** This provider cannot serve it. Another might. Move on immediately. */
  | 'failover';

export interface AttemptFailure {
  disposition: Disposition;
  status: number | null;
  code: string;
  message: string;
  /** From the provider's own reset header, when it sent one. */
  retryAfterMs: number | null;
}

/** Status codes that mean the caller's request is the problem. */
const FATAL_STATUSES = new Set([400, 404, 405, 413, 414, 415, 422, 501]);

/**
 * Codes that mean the connection failed rather than the request. Undici raises
 * these as errors with a `code`, never as a status.
 */
const RETRYABLE_ERROR_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'EPIPE',
  'ETIMEDOUT',
  'EAI_AGAIN',
  'ENOTFOUND',
  'UND_ERR_SOCKET',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_BODY_TIMEOUT',
]);

export function classifyStatus(
  status: number,
  profile: Pick<ProviderProfile, 'retryableStatus'>,
): Disposition {
  const override = profile.retryableStatus?.(status);
  if (override === true) return 'retry';
  if (override === false) return 'fatal';

  if (FATAL_STATUSES.has(status)) return 'fatal';

  // Our credential for this provider is wrong or unauthorised. That says
  // nothing about the next provider, which has its own key.
  if (status === 401 || status === 403) return 'failover';

  // This provider is out of quota now. Waiting for its window to reopen is
  // strictly worse than asking a provider that has quota available.
  if (status === 429) return 'failover';

  if (status === 408 || status === 409 || status === 425) return 'retry';

  if (status >= 500) return 'retry';

  // Any other 4xx is the caller's fault by default. Being wrong in this
  // direction returns one honest error; being wrong the other way multiplies it
  // across every provider.
  if (status >= 400) return 'fatal';

  return 'fatal';
}

export function classifyError(err: unknown): Disposition {
  const code = (err as { code?: string } | null)?.code;
  if (typeof code === 'string' && RETRYABLE_ERROR_CODES.has(code)) return 'retry';

  const name = (err as { name?: string } | null)?.name;
  // A caller-initiated abort is not a provider failure, and retrying it would
  // work against the client that just cancelled.
  if (name === 'AbortError') return 'fatal';

  return 'retry';
}

export function errorCode(err: unknown): string {
  const code = (err as { code?: string } | null)?.code;
  if (typeof code === 'string') return code;
  const name = (err as { name?: string } | null)?.name;
  return typeof name === 'string' ? name : 'unknown_error';
}

/** The error the gateway returns once every provider has been exhausted. */
export class AllProvidersFailedError extends Error {
  readonly failures: ReadonlyArray<{ provider: string } & AttemptFailure>;

  constructor(failures: ReadonlyArray<{ provider: string } & AttemptFailure>) {
    const summary = failures
      .map((f) => `${f.provider}: ${f.status ?? f.code}`)
      .join(', ');
    super(`every provider failed (${summary})`);
    this.name = 'AllProvidersFailedError';
    this.failures = failures;
  }
}
