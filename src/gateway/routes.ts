import { randomUUID } from 'node:crypto';
import { PassThrough } from 'node:stream';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import { z } from 'zod';
import { relayStream, type RelayResult } from '../streaming/relay.js';
import { ENGINE_VERSION, type Redactor } from '../audit/redact.js';
import type { AuditRecord, AuditWriter } from '../audit/writer.js';
import type { CompletionCache } from '../cache/completions.js';
import type { TokenBudget } from '../limits/budget.js';
import type { ProviderClient } from '../providers/client.js';
import type { ChatRequest, ProviderProfile } from '../providers/profile.js';
import { DEFAULT_MAX_TOKENS, countTokens, reservationFor } from '../tokens/counter.js';
import { apiKeyMatches, bearerToken } from './auth.js';
import type { Disposition } from '../providers/errors.js';
import { dispatch, type AttemptRecord, type DispatchResult } from './dispatch.js';
import type { BackoffOptions } from './backoff.js';

/**
 * One bucket per tenant rather than one per model.
 *
 * A budget is about cost, and a per-model bucket would hand a tenant a fresh
 * allowance for every model they name, multiplying the limit by the size of the
 * catalogue. The dimension is kept in the key so a future price-weighted split
 * has somewhere to go.
 */
const MODEL_CLASS = 'all';

/**
 * Only the fields the gateway itself reasons about are validated. Everything
 * else is forwarded untouched, because a proxy that strips unknown parameters
 * becomes the reason a caller cannot use a provider feature that shipped last
 * week. Provider-specific incompatibilities are handled by the profile's
 * dropParams instead, where they can be explained.
 */
const ChatRequestSchema = z
  .object({
    model: z.string().min(1),
    messages: z.array(z.record(z.string(), z.unknown())).min(1),
    stream: z.boolean().optional(),
    temperature: z.number().optional(),
    max_tokens: z.number().int().positive().optional(),
  })
  .loose();

export interface GatewayDeps {
  apiKey: string;
  providers: readonly ProviderProfile[];
  apiKeys: Readonly<Record<string, string | undefined>>;
  client: ProviderClient;
  maxAttemptsPerProvider: number;
  backoff: BackoffOptions;
  budget: TokenBudget;
  cache: CompletionCache;
  audit: AuditWriter;
  redactor: Redactor;
  deadlineMs: number;
  /**
   * How long to hold the headers waiting for a first content delta.
   *
   * The trade is the whole reason the commit point exists. Waiting keeps a
   * clean HTTP error available for longer; committing early gets bytes moving
   * sooner. Time to first token is the number a streaming API is judged on, so
   * this cannot be large.
   */
  commitDeadlineMs: number;
  /**
   * A single static key means a single tenant. Mapping keys to tenants is the
   * obvious extension and is left out on purpose: the budget below is already
   * multi-tenant, so only the lookup would change.
   */
  tenantId: string;
}

/**
 * What the request really cost, and how much that number can be trusted.
 *
 * A provider that sent no usage did not report zero, it reported nothing, so
 * the two have to stay distinguishable. Recording an estimate as though it came
 * from the provider is how token accounting quietly drifts.
 */
function actualUsage(
  result: DispatchResult,
  estimatedPrompt: number,
): { tokens: number; completion: number; source: 'provider' | 'estimated' } {
  const usage = result.success?.usage;
  const text = completionText(result);

  if (usage?.total_tokens !== undefined) {
    return {
      tokens: usage.total_tokens,
      completion: usage.completion_tokens ?? countTokens(text),
      source: 'provider',
    };
  }

  if (result.success !== null) {
    const completion = countTokens(text);
    return { tokens: estimatedPrompt + completion, completion, source: 'estimated' };
  }

  // Nothing was served. Failed attempts did cost the provider something, but no
  // number for it exists anywhere, so the reservation is returned in full. That
  // errs in the tenant's favour, which is the safer direction to be wrong in.
  return { tokens: 0, completion: 0, source: 'estimated' };
}

function completionText(result: DispatchResult): string {
  const choices = result.success?.body['choices'];
  const content = Array.isArray(choices)
    ? (choices[0] as { message?: { content?: unknown } } | undefined)?.message?.content
    : undefined;
  return typeof content === 'string' ? content : '';
}

export type ScheduleAudit = (
  partial: Pick<AuditRecord, 'outcome' | 'finalProvider' | 'cacheResult' | 'tokenSource'> & {
    estPromptTokens?: number | null;
    promptTokens?: number | null;
    completionTokens?: number | null;
    rawCompletion?: string | null;
    attempts?: readonly AttemptRecord[];
  },
) => void;

/**
 * The status to answer with once no provider could serve the request.
 *
 * A rate limit is passed through rather than flattened into a 502. The two ask
 * the caller for different things: 502 says the upstream is broken and invites
 * an alert, while 429 with a retry-after says the quota is spent and tells them
 * exactly when to come back. Reporting a saturated provider as a fault sends
 * someone debugging a healthy service.
 */
function statusForFailure(failure: { disposition: Disposition; status: number | null }): number {
  if (failure.disposition === 'fatal') return failure.status ?? 400;
  if (failure.status === 429) return 429;
  return 502;
}

/**
 * Where the recorded token count came from.
 *
 * `partial_estimated` is not a rare case: an interrupted or cancelled stream is
 * precisely when the provider's usage frame never arrives, and that is the same
 * moment the gateway most needs a number. Recording it as though the provider
 * had said so is how accounting drifts without anyone noticing.
 */
function tokenSourceForAudit(result: RelayResult): AuditRecord['tokenSource'] {
  if (result.tokenSource === 'provider') return 'provider';
  const interrupted =
    result.outcome === 'stream_abort' ||
    result.outcome === 'in_band_error' ||
    result.outcome === 'client_gone';
  return interrupted ? 'partial_estimated' : 'estimated';
}

/** How a stream ended, in the vocabulary the audit table uses. */
function outcomeForAudit(result: RelayResult): AuditRecord['outcome'] {
  switch (result.outcome) {
    case 'complete':
      return 'ok';
    case 'truncated_clean':
      return 'truncated';
    case 'client_gone':
      return 'client_abort';
    default:
      // Committed means the caller already has part of an answer, so the
      // request was truncated rather than simply failed. Uncommitted means they
      // got a clean error and nothing else.
      return result.committed ? 'truncated' : 'failed';
  }
}

/**
 * The streaming path.
 *
 * The shape that matters: nothing is written to the client until the relay
 * judges the stream live. Up to that point a clean HTTP error is still
 * available, and past it the status is already 200 and every failure has to
 * travel inside the stream.
 */
async function streamCompletion(args: {
  deps: GatewayDeps;
  request: FastifyRequest;
  reply: FastifyReply;
  chatRequest: ChatRequest;
  requestId: string;
  schedule: ScheduleAudit;
  flushAudit: (request: FastifyRequest, statusCode: number) => Promise<void>;
}): Promise<FastifyReply> {
  const { deps, request, reply, chatRequest, requestId, schedule, flushAudit } = args;

  const { prompt, reserve } = reservationFor(chatRequest);
  const gate = await deps.budget.reserve(deps.tenantId, MODEL_CLASS, requestId, reserve);

  if (gate.impossible) {
    schedule({
      outcome: 'failed',
      finalProvider: null,
      cacheResult: 'bypass',
      tokenSource: 'estimated',
      estPromptTokens: reserve,
    });
    return reply.code(400).send({
      error: {
        message: `this request reserves about ${reserve} tokens, which is more than the budget can ever hold`,
        type: 'invalid_request_error',
        code: 'request_exceeds_budget',
      },
    });
  }

  if (!gate.admitted) {
    schedule({
      outcome: 'failed',
      finalProvider: null,
      cacheResult: 'bypass',
      tokenSource: 'estimated',
      estPromptTokens: reserve,
    });
    return reply
      .header('retry-after', String(Math.ceil(gate.retryAfterMs / 1_000)))
      .code(429)
      .send({
        error: {
          message: 'token budget exhausted for this tenant',
          type: 'rate_limit_exceeded',
          code: 'token_budget_exhausted',
        },
      });
  }

  const profile = deps.providers[0];
  if (profile === undefined) {
    await deps.budget.settle(deps.tenantId, MODEL_CLASS, requestId, reserve, 0);
    schedule({
      outcome: 'failed',
      finalProvider: null,
      cacheResult: 'bypass',
      tokenSource: 'estimated',
    });
    return reply.code(502).send({
      error: { message: 'no providers are configured', type: 'configuration_error' },
    });
  }

  const controller = new AbortController();
  const onClose = (): void => {
    if (!reply.raw.writableFinished) controller.abort();
  };
  reply.raw.on('close', onClose);

  const opened = await deps.client.openStream(profile, chatRequest, {
    apiKey: deps.apiKeys[profile.name],
    signal: controller.signal,
  });

  if (!opened.ok) {
    // Nothing has been written, so the caller can still be told plainly what
    // went wrong instead of receiving a 200 that carries an error inside it.
    reply.raw.off('close', onClose);
    await deps.budget.settle(deps.tenantId, MODEL_CLASS, requestId, reserve, 0);
    schedule({
      outcome: 'failed',
      finalProvider: opened.provider,
      cacheResult: 'bypass',
      tokenSource: 'estimated',
      estPromptTokens: reserve,
      attempts: [
        {
          attemptNo: 1,
          provider: opened.provider,
          outcome: opened.status === 429 ? 'rate_limited' : 'http_error',
          httpStatus: opened.status,
          errorCode: opened.code,
          latencyMs: opened.latencyMs,
          committed: false,
          providerRequestId: null,
        },
      ],
    });
    const status = statusForFailure(opened);
    if (status === 429 && opened.retryAfterMs !== null) {
      void reply.header('retry-after', String(Math.ceil(opened.retryAfterMs / 1_000)));
    }
    return reply.code(status).send({
      error: {
        message: opened.message,
        type: opened.disposition === 'fatal' ? 'invalid_request_error' : 'upstream_error',
        code: opened.code,
      },
    });
  }

  // The only bound on how much of a slow client's backlog is held in memory.
  const passThrough = new PassThrough({ highWaterMark: 64 * 1024 });

  const result = await relayStream({
    body: opened.body,
    commitDeadlineMs: deps.commitDeadlineMs,
    signal: controller.signal,
    countTokens: (text) => countTokens(text),
    // Asked of the profile, because where a provider puts usage is a provider
    // fact and belongs in exactly one place.
    extractUsage: (chunk) => profile.extractUsage(chunk),
    // A request that named no ceiling still gets one. The budget already
    // reserved this much, and a model that ignores max_tokens would otherwise
    // bill a tenant for a generation nobody bounded.
    maxCompletionTokens: chatRequest.max_tokens ?? DEFAULT_MAX_TOKENS,
    commit: () => {
      void reply
        .header('content-type', 'text/event-stream; charset=utf-8')
        // no-transform is what stops a compression layer collapsing the stream
        // into one burst, which no assertion on the frames would ever catch.
        .header('cache-control', 'no-cache, no-transform')
        .header('connection', 'keep-alive')
        .header('x-accel-buffering', 'no')
        .header('x-modelgate-provider', opened.provider)
        .header('x-modelgate-cache', 'bypass')
        .code(200)
        .send(passThrough);
      return passThrough;
    },
  });

  reply.raw.off('close', onClose);

  const promptTokens = result.providerUsage?.prompt_tokens ?? prompt;
  const spent = result.providerUsage?.total_tokens ?? promptTokens + result.completionTokens;
  await deps.budget.settle(deps.tenantId, MODEL_CLASS, requestId, reserve, spent);

  schedule({
    outcome: outcomeForAudit(result),
    finalProvider: opened.provider,
    cacheResult: 'bypass',
    tokenSource: tokenSourceForAudit(result),
    estPromptTokens: reserve,
    promptTokens,
    completionTokens: result.completionTokens,
    rawCompletion: result.completionText,
    attempts: [
      {
        attemptNo: 1,
        provider: opened.provider,
        outcome: result.outcome === 'stream_abort' ? 'stream_abort'
          : result.outcome === 'truncated_clean' ? 'truncated_clean'
          : result.outcome === 'in_band_error' ? 'http_error'
          : 'ok',
        httpStatus: opened.status,
        errorCode: result.errorMessage === null ? null : 'upstream_stream_error',
        latencyMs: opened.latencyMs,
        committed: result.committed,
        providerRequestId: null,
      },
    ],
  });

  if (!result.committed) {
    // The stream opened and then died without producing anything. Nothing has
    // been sent, so this can still be an honest error rather than an empty 200.
    return reply.code(502).send({
      error: {
        message: result.errorMessage ?? 'the provider opened a stream and sent nothing',
        type: 'upstream_error',
        code: 'empty_stream',
      },
    });
  }

  // Ending the sink is what told Fastify the response was done, so the
  // onResponse hook has very likely already run and found nothing pending.
  await flushAudit(request, reply.statusCode);
  return reply;
}

/** Everything the audit row needs, captured before the response is sent. */
interface PendingAudit {
  record: Omit<AuditRecord, 'redactionClasses' | 'entropySuspect'>;
  rawPrompt: string;
  rawCompletion: string | null;
  attempts: readonly AttemptRecord[];
}

export const gatewayRoutes: FastifyPluginAsync<GatewayDeps> = async (
  app: FastifyInstance,
  deps: GatewayDeps,
) => {
  // Keyed by request rather than decorated onto it, so nothing is shared
  // between requests by accident and the entry disappears with the request.
  const pending = new WeakMap<FastifyRequest, PendingAudit>();

  app.addHook('onRequest', async (request, reply) => {
    const token = bearerToken(request.headers.authorization);
    if (token === null || !apiKeyMatches(token, deps.apiKey)) {
      return reply.code(401).send({
        error: { message: 'invalid or missing api key', type: 'authentication_error' },
      });
    }
  });

  /**
   * Redaction and the database write both happen here, after the response has
   * gone out. Neither belongs on the caller's latency, and the hook runs for
   * every reply mode, so a request cannot finish without being recorded.
   */
  /**
   * Writes whatever is pending for this request, once.
   *
   * Called from the onResponse hook and again when a stream finishes, because
   * the two race. Ending the relay's sink is exactly what makes Fastify
   * consider the response complete, so for a streaming request the hook fires
   * before the handler has finished working out what to record. Whichever
   * arrives second finds the map empty and does nothing.
   */
  const flushAudit = async (request: FastifyRequest, statusCode: number): Promise<void> => {
    const entry = pending.get(request);
    if (entry === undefined) return;
    pending.delete(request);

    try {
      const redactedPrompt = deps.redactor.redact(entry.rawPrompt);
      const redactedCompletion =
        entry.rawCompletion === null ? null : deps.redactor.redact(entry.rawCompletion);

      await deps.audit.write(
        {
          ...entry.record,
          httpStatus: statusCode,
          redactionClasses: [
            ...new Set([...redactedPrompt.classes, ...(redactedCompletion?.classes ?? [])]),
          ],
          entropySuspect: redactedPrompt.entropySuspect || (redactedCompletion?.entropySuspect ?? false),
        },
        {
          prompt: redactedPrompt.text,
          completion: redactedCompletion?.text ?? null,
          promptFingerprint: deps.redactor.fingerprint(entry.rawPrompt),
          redactions: [...redactedPrompt.entries, ...(redactedCompletion?.entries ?? [])],
          engineVersion: ENGINE_VERSION,
        },
        entry.attempts,
      );
    } catch (err) {
      // The response has already been sent, so there is nobody left to tell.
      // Logged with the request id rather than swallowed, because a gap in the
      // audit trail has to be explainable afterwards.
      request.log.error({ err, requestId: entry.record.requestId }, 'audit write failed');
    }
  };

  app.addHook('onResponse', async (request, reply) => {
    await flushAudit(request, reply.statusCode);
  });

  app.post('/v1/chat/completions', async (request, reply) => {
    const startedAt = Date.now();

    const parsed = ChatRequestSchema.safeParse(request.body);
    if (!parsed.success) {
      const issue = parsed.error.issues[0];
      return reply.code(400).send({
        error: {
          message: `${issue?.path.join('.') || 'body'}: ${issue?.message ?? 'invalid request'}`,
          type: 'invalid_request_error',
        },
      });
    }

    const chatRequest = parsed.data as ChatRequest;
    const requestId = randomUUID();
    // The messages as sent, so a secret in any field is seen by the redactor
    // rather than only the ones a schema happened to name.
    const rawPrompt = JSON.stringify(chatRequest.messages);
    const idempotencyKey = headerValue(request.headers['idempotency-key']);

    const schedule = (
      partial: Pick<
        AuditRecord,
        'outcome' | 'finalProvider' | 'cacheResult' | 'tokenSource'
      > & {
        estPromptTokens?: number | null;
        promptTokens?: number | null;
        completionTokens?: number | null;
        rawCompletion?: string | null;
        attempts?: readonly AttemptRecord[];
      },
    ): void => {
      pending.set(request, {
        record: {
          requestId,
          tenantId: deps.tenantId,
          idempotencyKey,
          model: chatRequest.model,
          stream: false,
          outcome: partial.outcome,
          httpStatus: null,
          finalProvider: partial.finalProvider,
          cacheResult: partial.cacheResult,
          estPromptTokens: partial.estPromptTokens ?? null,
          promptTokens: partial.promptTokens ?? null,
          completionTokens: partial.completionTokens ?? null,
          tokenSource: partial.tokenSource,
          ttfbMs: null,
          totalMs: Date.now() - startedAt,
        },
        rawPrompt,
        rawCompletion: partial.rawCompletion ?? null,
        attempts: partial.attempts ?? [],
      });
    };

    if (chatRequest.stream === true) {
      return streamCompletion({
        deps,
        request,
        reply,
        chatRequest,
        requestId,
        schedule,
        flushAudit,
      });
    }

    // Looked up before the budget is touched, because a hit costs nothing
    // upstream and charging a tenant's tokens for work no provider did would be
    // charging them for the gateway's own memory.
    const cacheKey = deps.cache.key(chatRequest, deps.tenantId);
    const cached = await deps.cache.get(cacheKey);
    if (cached.body !== null) {
      schedule({
        outcome: 'ok',
        finalProvider: null,
        cacheResult: 'hit',
        tokenSource: 'estimated',
        rawCompletion: completionText({ success: { body: cached.body } } as DispatchResult),
      });
      return reply
        .header('x-modelgate-cache', 'hit')
        .header('x-modelgate-attempts', '0')
        .code(200)
        .send(cached.body);
    }

    const { prompt, reserve } = reservationFor(chatRequest);

    // The true cost is unknown until the response ends, so the worst case is
    // held first and the difference given back afterwards. Checking afterwards
    // instead would admit every concurrent request on the same stale reading.
    const gate = await deps.budget.reserve(deps.tenantId, MODEL_CLASS, requestId, reserve);

    if (gate.impossible) {
      // A different answer from "not right now". No amount of waiting makes a
      // request larger than the whole bucket fit, so a 429 would be telling the
      // caller to retry something that can never succeed.
      schedule({
        outcome: 'failed',
        finalProvider: null,
        cacheResult: 'miss',
        tokenSource: 'estimated',
        estPromptTokens: reserve,
      });
      return reply.code(400).send({
        error: {
          message: `this request reserves about ${reserve} tokens, which is more than the budget can ever hold`,
          type: 'invalid_request_error',
          code: 'request_exceeds_budget',
        },
      });
    }

    if (!gate.admitted) {
      schedule({
        outcome: 'failed',
        finalProvider: null,
        cacheResult: 'miss',
        tokenSource: 'estimated',
        estPromptTokens: reserve,
      });
      return reply
        .header('retry-after', String(Math.ceil(gate.retryAfterMs / 1_000)))
        .header('x-modelgate-tokens-remaining', String(gate.remaining))
        .code(429)
        .send({
          error: {
            message: 'token budget exhausted for this tenant',
            type: 'rate_limit_exceeded',
            code: 'token_budget_exhausted',
          },
        });
    }

    // Once the caller has hung up there is nobody to answer, and continuing
    // through the chain spends provider calls on a response that will be
    // discarded. `close` also fires on a normal finish, so the guard is whether
    // the response actually completed.
    const controller = new AbortController();
    const onClose = (): void => {
      if (!reply.raw.writableFinished) controller.abort();
    };
    reply.raw.on('close', onClose);

    let result: DispatchResult | null = null;
    let remaining = gate.remaining;
    let used = { tokens: 0, completion: 0, source: 'estimated' as 'provider' | 'estimated' };
    try {
      result = await dispatch(chatRequest, {
        providers: deps.providers,
        client: deps.client,
        apiKeys: deps.apiKeys,
        maxAttemptsPerProvider: deps.maxAttemptsPerProvider,
        backoff: deps.backoff,
        deadlineMs: deps.deadlineMs,
        signal: controller.signal,
      });
    } finally {
      reply.raw.off('close', onClose);
      // In a finally so an unexpected throw still returns the reservation
      // rather than leaving it for the sweep to reclaim a lease TTL later.
      used = result === null ? used : actualUsage(result, prompt);
      remaining = (
        await deps.budget.settle(deps.tenantId, MODEL_CLASS, requestId, reserve, used.tokens)
      ).remaining;
    }

    // Named so a demo can show which provider actually answered, and so a
    // failover is visible without reading the audit table.
    void reply.header('x-modelgate-attempts', String(result.attempts.length));
    void reply.header('x-modelgate-tokens-remaining', String(remaining));
    void reply.header('x-modelgate-token-source', used.source);

    if (result.success !== null) {
      await deps.cache.set(cacheKey, result.success.body);
      schedule({
        outcome: 'ok',
        finalProvider: result.success.provider,
        cacheResult: 'miss',
        tokenSource: used.source,
        estPromptTokens: reserve,
        promptTokens: result.success.usage?.prompt_tokens ?? prompt,
        completionTokens: used.completion,
        rawCompletion: completionText(result),
        attempts: result.attempts,
      });
      return reply
        .header('x-modelgate-provider', result.success.provider)
        .header('x-modelgate-cache', 'miss')
        .code(200)
        .send(result.success.body);
    }

    const failure = result.failure;
    if (failure === null) {
      schedule({
        outcome: 'failed',
        finalProvider: null,
        cacheResult: 'miss',
        tokenSource: 'estimated',
        estPromptTokens: reserve,
        attempts: result.attempts,
      });
      return reply.code(502).send({
        error: { message: 'no providers are configured', type: 'configuration_error' },
      });
    }

    const status = statusForFailure(failure);
    if (status === 429 && failure.retryAfterMs !== null) {
      void reply.header('retry-after', String(Math.ceil(failure.retryAfterMs / 1_000)));
    }

    schedule({
      outcome: 'failed',
      finalProvider: failure.provider,
      cacheResult: 'miss',
      tokenSource: 'estimated',
      estPromptTokens: reserve,
      attempts: result.attempts,
    });

    return reply
      .header('x-modelgate-provider', failure.provider)
      .code(status)
      .send({
        error: {
          message: failure.message,
          type: failure.disposition === 'fatal' ? 'invalid_request_error' : 'upstream_error',
          code: failure.code,
        },
      });
  });
};

function headerValue(value: string | string[] | undefined): string | null {
  const raw = Array.isArray(value) ? value[0] : value;
  return typeof raw === 'string' && raw.trim() !== '' ? raw.trim() : null;
}
