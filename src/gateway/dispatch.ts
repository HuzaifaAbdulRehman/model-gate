import { setTimeout as delay } from 'node:timers/promises';
import type { AttemptError, AttemptSuccess, ProviderClient } from '../providers/client.js';
import type { ChatRequest, ProviderProfile } from '../providers/profile.js';
import { retryDelay, type BackoffOptions } from './backoff.js';

/** Shaped to go straight into the request_attempts table. */
export interface AttemptRecord {
  attemptNo: number;
  provider: string;
  outcome: 'ok' | 'http_error' | 'timeout' | 'stream_abort' | 'rate_limited' | 'truncated_clean';
  httpStatus: number | null;
  errorCode: string | null;
  latencyMs: number;
  /** Always false while the gateway is non-streaming: nothing reaches the client early. */
  committed: boolean;
  providerRequestId: string | null;
}

export interface DispatchOptions {
  providers: readonly ProviderProfile[];
  client: ProviderClient;
  apiKeys?: Readonly<Record<string, string | undefined>>;
  maxAttemptsPerProvider: number;
  backoff: BackoffOptions;
  /**
   * A ceiling on the whole chain, not on one call.
   *
   * Each provider call is bounded on its own, but the worst case is providers
   * times attempts times timeout plus backoff, and a caller should not wait all
   * of that for a 502 that became inevitable at the first provider.
   */
  deadlineMs?: number;
  signal?: AbortSignal | undefined;
  /** Injectable so retry tests do not spend the backoff in real time. */
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

export interface DispatchResult {
  success: AttemptSuccess | null;
  /** The error to return when nothing succeeded. */
  failure: AttemptError | null;
  attempts: AttemptRecord[];
}

const TIMEOUT_CODES = new Set([
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_BODY_TIMEOUT',
  'UND_ERR_CONNECT_TIMEOUT',
  'ETIMEDOUT',
]);

/**
 * Attributed to the gateway rather than to a provider, because no provider
 * failed here. The chain simply ran out of time, and blaming whichever one was
 * next would send someone debugging a healthy service.
 */
function deadlineFailure(elapsedMs: number): AttemptError {
  return {
    ok: false,
    provider: 'modelgate',
    disposition: 'failover',
    status: null,
    code: 'gateway_deadline_exceeded',
    message: `the provider chain did not answer within the deadline (${elapsedMs}ms)`,
    retryAfterMs: null,
    latencyMs: elapsedMs,
  };
}

function outcomeOf(failure: AttemptError): AttemptRecord['outcome'] {
  if (failure.status === 429) return 'rate_limited';
  if (TIMEOUT_CODES.has(failure.code)) return 'timeout';
  return 'http_error';
}

/**
 * Walks the provider list, retrying transient failures on the same provider
 * before moving on.
 *
 * The ordering rule that matters: a fatal failure stops everything. A request
 * the provider rejected as malformed will be rejected by every other provider
 * too, so continuing would turn one clean 400 into a handful of pointless calls
 * and an error message naming the wrong provider.
 */
export async function dispatch(
  request: ChatRequest,
  options: DispatchOptions,
): Promise<DispatchResult> {
  const sleep = options.sleep ?? ((ms: number) => delay(ms));
  const now = options.now ?? Date.now;
  const startedAt = now();
  const attempts: AttemptRecord[] = [];
  let lastFailure: AttemptError | null = null;
  let attemptNo = 0;

  // Checked before starting an attempt rather than during one. Cutting a call
  // that is already in flight would waste the work without saving the wait.
  const outOfTime = (): boolean =>
    options.deadlineMs !== undefined && now() - startedAt >= options.deadlineMs;

  for (const profile of options.providers) {
    for (let tries = 0; tries < options.maxAttemptsPerProvider; tries += 1) {
      if (outOfTime()) {
        return { success: null, failure: lastFailure ?? deadlineFailure(now() - startedAt), attempts };
      }
      attemptNo += 1;

      const result = await options.client.call(profile, request, {
        apiKey: options.apiKeys?.[profile.name],
        signal: options.signal,
      });

      if (result.ok) {
        attempts.push({
          attemptNo,
          provider: result.provider,
          outcome: 'ok',
          httpStatus: result.status,
          errorCode: null,
          latencyMs: result.latencyMs,
          committed: false,
          providerRequestId: result.providerRequestId,
        });
        return { success: result, failure: null, attempts };
      }

      lastFailure = result;
      attempts.push({
        attemptNo,
        provider: result.provider,
        outcome: outcomeOf(result),
        httpStatus: result.status,
        errorCode: result.code,
        latencyMs: result.latencyMs,
        committed: false,
        providerRequestId: null,
      });

      if (result.disposition === 'fatal') {
        return { success: null, failure: result, attempts };
      }

      if (result.disposition === 'failover') break;

      const isLastTry = tries === options.maxAttemptsPerProvider - 1;
      if (isLastTry) break;

      await sleep(retryDelay(tries, result.retryAfterMs, options.backoff));
    }
  }

  return { success: null, failure: lastFailure, attempts };
}
