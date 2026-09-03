import { timingSafeEqual } from 'node:crypto';

/**
 * Compares the presented key against the configured one without leaking its
 * length or its matching prefix through timing.
 *
 * `timingSafeEqual` throws on a length mismatch, so the lengths are compared
 * first and both branches still run a comparison of equal cost. A plain `===`
 * would return on the first differing byte, and a `startsWith` would also
 * accept any key with the real one as a prefix.
 */
export function apiKeyMatches(presented: string, expected: string): boolean {
  const a = Buffer.from(presented, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  if (a.length !== b.length) {
    timingSafeEqual(b, b);
    return false;
  }
  return timingSafeEqual(a, b);
}

/** Pulls the bearer token out of an Authorization header, if there is one. */
export function bearerToken(header: string | string[] | undefined): string | null {
  const raw = Array.isArray(header) ? header[0] : header;
  if (typeof raw !== 'string') return null;
  const match = /^Bearer[ ]+(.+)$/.exec(raw.trim());
  return match?.[1] ?? null;
}
