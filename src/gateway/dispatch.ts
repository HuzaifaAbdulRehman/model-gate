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
  signal?: AbortSignal | undefined;
  /** Injectable so retry tests do not spend the backoff in real time. */
  sleep?: (ms: number) => Promise<void>;
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
  const attempts: AttemptRecord[] = [];
  let lastFailure: AttemptError | null = null;
  let attemptNo = 0;

  for (const profile of options.providers) {
    for (let tries = 0; tries < options.maxAttemptsPerProvider; tries += 1) {
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
