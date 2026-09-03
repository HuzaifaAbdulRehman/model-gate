/**
 * One adapter, many providers.
 *
 * Roughly all of the request and response handling is identical across
 * OpenAI-wire providers; what differs is data, not behaviour. So the differences
 * live in a profile object rather than in a second adapter or a class
 * hierarchy, which would give the usage-extraction bug two places to hide.
 */

export interface Usage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
}

export interface ChatRequest {
  model: string;
  messages: Array<Record<string, unknown>>;
  stream?: boolean;
  temperature?: number;
  max_tokens?: number;
  response_format?: unknown;
  [key: string]: unknown;
}

export interface RateLimitSnapshot {
  remainingRequests: number | null;
  remainingTokens: number | null;
  /** Milliseconds until the window resets, or null when the provider said nothing. */
  resetMs: number | null;
}

export interface ProviderProfile {
  name: string;
  origin: string;
  path: string;
  /** Empty when the provider needs no credential, as the local mock does. */
  authHeader: (key: string | undefined) => Record<string, string>;
  /** Parameters this provider rejects or silently ignores. Removed before sending. */
  dropParams: readonly string[];
  /** Parameters pinned regardless of what the caller asked for. */
  forceParams: Readonly<Record<string, unknown>>;
  /**
   * Clamped here rather than left to the provider, so the audit log records the
   * value that was actually used instead of the one that was asked for.
   */
  clampTemperatureMin?: number;
  /**
   * Combinations this provider rejects outright. Checked before dispatch so the
   * result is one clean 400 rather than the same 400 from every provider in
   * turn.
   */
  rejectCombos: ReadonlyArray<(request: ChatRequest) => string | null>;
  /** Where this provider puts usage. Never assume the top level. */
  extractUsage: (body: unknown) => Usage | null;
  parseRateLimitHeaders: (headers: Record<string, string | string[] | undefined>) => RateLimitSnapshot;
  /** Provider-specific overrides on top of the shared status classification. */
  retryableStatus?: (status: number) => boolean | undefined;
  timeouts: { headers: number; body: number };
}

function header(
  headers: Record<string, string | string[] | undefined>,
  name: string,
): string | undefined {
  const value = headers[name];
  return Array.isArray(value) ? value[0] : value;
}

function numberOrNull(raw: string | undefined): number | null {
  if (raw === undefined) return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

/**
 * Parses a Go duration such as `2m59.56s` or `7.66s`.
 *
 * Groq reports resets in this format, and `parseInt('2m59.56s')` returns 2,
 * which would be read as two seconds instead of a hundred and eighty. A wrong
 * value here is worse than none: the gateway would retry into a closed window
 * and burn the remaining quota.
 */
export function parseGoDuration(raw: string | undefined): number | null {
  if (raw === undefined) return null;

  const trimmed = raw.trim();
  if (trimmed === '') return null;

  // A bare number is seconds, which is what most providers send.
  if (/^\d+(\.\d+)?$/.test(trimmed)) return Math.round(Number(trimmed) * 1000);

  const pattern = /(\d+(?:\.\d+)?)(ms|us|µs|ns|[smh])/g;
  const units: Record<string, number> = {
    ns: 1e-6, us: 1e-3, 'µs': 1e-3, ms: 1, s: 1000, m: 60_000, h: 3_600_000,
  };

  let total = 0;
  let matched = false;
  for (const match of trimmed.matchAll(pattern)) {
    const value = Number(match[1]);
    const unit = units[match[2] ?? ''];
    if (unit === undefined || !Number.isFinite(value)) continue;
    total += value * unit;
    matched = true;
  }

  return matched ? Math.round(total) : null;
}

const openAiStyleRateLimits = (
  headers: Record<string, string | string[] | undefined>,
): RateLimitSnapshot => ({
  remainingRequests: numberOrNull(header(headers, 'x-ratelimit-remaining-requests')),
  remainingTokens: numberOrNull(header(headers, 'x-ratelimit-remaining-tokens')),
  resetMs: parseGoDuration(header(headers, 'x-ratelimit-reset-requests')),
});

/**
 * Usage on a non-streaming body. Groq documents `x_groq` for exactly this shape,
 * and checking both costs nothing while assuming one silently records zero.
 */
const usageFromBody = (body: unknown): Usage | null => {
  if (typeof body !== 'object' || body === null) return null;
  const record = body as { usage?: Usage; x_groq?: { usage?: Usage } };
  return record.usage ?? record.x_groq?.usage ?? null;
};

export interface Timeouts {
  headers: number;
  body: number;
}

const DEFAULT_TIMEOUTS: Timeouts = { headers: 15_000, body: 20_000 };

export function mockProfile(
  name: string,
  origin: string,
  timeouts: Timeouts = DEFAULT_TIMEOUTS,
): ProviderProfile {
  return {
    name,
    origin,
    path: '/v1/chat/completions',
    authHeader: () => ({}),
    dropParams: [],
    forceParams: {},
    rejectCombos: [],
    extractUsage: usageFromBody,
    parseRateLimitHeaders: openAiStyleRateLimits,
    timeouts,
  };
}

export function groqProfile(
  origin = 'https://api.groq.com',
  timeouts: Timeouts = DEFAULT_TIMEOUTS,
): ProviderProfile {
  return {
    name: 'groq',
    origin,
    path: '/openai/v1/chat/completions',
    authHeader: (key) => (key === undefined ? {} : { authorization: `Bearer ${key}` }),
    // Accepted by OpenAI and rejected or ignored by Groq. Dropping them here
    // means the caller's request works on either provider unchanged.
    dropParams: ['logprobs', 'top_logprobs', 'logit_bias'],
    forceParams: { n: 1 },
    clampTemperatureMin: 1e-8,
    rejectCombos: [
      (request) =>
        request.stream === true && request.response_format !== undefined
          ? 'groq rejects response_format together with streaming'
          : null,
    ],
    extractUsage: usageFromBody,
    parseRateLimitHeaders: openAiStyleRateLimits,
    // Groq documents 498 as retryable and 499 as never retryable, which is the
    // reverse of what the numbers suggest.
    retryableStatus: (status) =>
      status === 498 ? true : status === 499 ? false : undefined,
    timeouts,
  };
}

/**
 * Applies the profile to a caller's request. Returns the body to send, or a
 * rejection reason when this provider will certainly refuse it.
 */
export function shapeRequest(
  profile: ProviderProfile,
  request: ChatRequest,
): { body: ChatRequest } | { reject: string } {
  for (const check of profile.rejectCombos) {
    const reason = check(request);
    if (reason !== null) return { reject: reason };
  }

  const body: ChatRequest = { ...request };
  for (const param of profile.dropParams) delete body[param];
  Object.assign(body, profile.forceParams);

  if (
    profile.clampTemperatureMin !== undefined &&
    typeof body.temperature === 'number' &&
    body.temperature < profile.clampTemperatureMin
  ) {
    body.temperature = profile.clampTemperatureMin;
  }

  return { body };
}
