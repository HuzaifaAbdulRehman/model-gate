/**
 * Failure selection for the mock provider.
 *
 * A test names the failure in a request header, so one running server drives
 * every case and no test has to stand up its own broken variant.
 */

export const FAILURE_KINDS = [
  'none',
  /** Destroy the socket after K content frames. No finish frame, no [DONE]. */
  'abort',
  /** Half-close after K frames. Reads as a socket error too, by a different route. */
  'fin',
  /**
   * A well-formed end of response that is not the end of the answer: the socket
   * closes cleanly after K frames with no finish_reason and no [DONE]. No error
   * is raised and no timeout fires, so an iterator simply finishes early.
   */
  'clean-end',
  /** Write half of a `data: {` and destroy, so the parser meets a partial frame. */
  'truncate-json',
  /** In-band `data:` error object, then destroy. */
  'error-data',
  /** In-band `event: error` frame, then destroy. */
  'error-event',
  /** Groq's early stop: a finish chunk carrying x_groq.error. */
  'x-groq-error',
  /** Protocol-perfect and semantically truncated: content_filter plus [DONE]. */
  'filter-stop',
  /** The same class, hitting the token ceiling instead. */
  'length-stop',
  /** Headers and K frames, then silence. Only an idle timer catches this. */
  'hang',
  /** Usage was requested but the stream ends without ever sending it. */
  'no-usage',
  /** Interleave choice-less frames, which break anything reaching for choices[0]. */
  'noise',
  /** 429 with Groq-shaped headers, including a Go duration in the reset. */
  'rate-limit',
  /**
   * A 400 the provider blames on the request. Every provider would answer the
   * same way, so this is the case a gateway must NOT fail over on.
   */
  'bad-request',
  /** 500. Transient by nature, so worth another try at the same provider. */
  'server-error',
  /** Fixed delay between frames, for backpressure and time-to-first-token work. */
  'slow',
  /** Tool call arguments split mid-escape and mid-UTF-8 character. */
  'tool-split',
] as const;

export type FailureKind = (typeof FAILURE_KINDS)[number];

export interface Failure {
  kind: FailureKind;
  /** Frame index for the `@K` modes, milliseconds for `slow`. Zero otherwise. */
  at: number;
}

const NO_FAILURE: Failure = { kind: 'none', at: 0 };

/** Aliases so a test can write what the wire actually shows. */
const ALIASES: Record<string, FailureKind> = {
  '429': 'rate-limit',
  'ratelimit': 'rate-limit',
};

/**
 * Throws on anything unrecognised rather than falling back to "no failure". A
 * mistyped mode that silently disabled the fault would leave a test asserting
 * failover behaviour against a perfectly healthy stream, and it would pass.
 */
export function parseFailure(header: string | undefined): Failure {
  if (header === undefined || header.trim() === '') return NO_FAILURE;

  const raw = header.trim();
  const [namePart, atPart] = raw.split('@');
  const name = (namePart ?? '').trim();
  const kind = ALIASES[name] ?? (name as FailureKind);

  if (!FAILURE_KINDS.includes(kind)) {
    throw new Error(
      `unknown x-mock-fail mode ${JSON.stringify(name)}; expected one of ${FAILURE_KINDS.join(', ')}`,
    );
  }

  if (atPart === undefined) return { kind, at: 0 };

  const at = Number(atPart);
  if (!Number.isInteger(at) || at < 0) {
    throw new Error(`x-mock-fail ${name}@ expects a non-negative integer, got ${JSON.stringify(atPart)}`);
  }

  return { kind, at };
}
