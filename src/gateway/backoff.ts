export interface BackoffOptions {
  baseMs: number;
  capMs: number;
  /** Injectable so tests are deterministic. Returns [0, 1). */
  random?: () => number;
}

/**
 * Full jitter: a uniform draw from zero up to the exponential ceiling.
 *
 * The exponential part is what stops a struggling provider being hammered; the
 * jitter is what stops every client that failed at the same moment retrying at
 * the same moment. Without it, a fleet phase-locks into a periodic spike and
 * the provider never gets a quiet window to recover in.
 */
export function backoffDelay(attempt: number, options: BackoffOptions): number {
  const random = options.random ?? Math.random;
  const ceiling = Math.min(options.capMs, options.baseMs * 2 ** Math.max(0, attempt));
  return Math.floor(random() * ceiling);
}

/**
 * A provider that told us when to come back is more accurate than any local
 * guess, so its own reset wins. Still capped: a rate limit reset minutes away
 * would hold the caller's request open far longer than they will wait, and the
 * right answer then is to move to another provider, not to sleep.
 */
export function retryDelay(
  attempt: number,
  retryAfterMs: number | null,
  options: BackoffOptions,
): number {
  if (retryAfterMs !== null && retryAfterMs >= 0) {
    return Math.min(retryAfterMs, options.capMs);
  }
  return backoffDelay(attempt, options);
}
