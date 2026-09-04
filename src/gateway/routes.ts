import { randomUUID } from 'node:crypto';
import type { FastifyInstance, FastifyPluginAsync, FastifyRequest } from 'fastify';
import { z } from 'zod';
import { ENGINE_VERSION, type Redactor } from '../audit/redact.js';
import type { AuditRecord, AuditWriter } from '../audit/writer.js';
import type { CompletionCache } from '../cache/completions.js';
import type { TokenBudget } from '../limits/budget.js';
import type { ProviderClient } from '../providers/client.js';
import type { ChatRequest, ProviderProfile } from '../providers/profile.js';
import { countTokens, reservationFor } from '../tokens/counter.js';
import { apiKeyMatches, bearerToken } from './auth.js';
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
  app.addHook('onResponse', async (request, reply) => {
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
          httpStatus: reply.statusCode,
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

    // Streaming lands in phase 3. Saying so is better than accepting the flag
    // and silently returning a non-streamed body, which a caller would only
    // discover by watching nothing arrive incrementally.
    if (parsed.data.stream === true) {
      return reply.code(501).send({
        error: {
          message: 'streaming is not implemented yet; omit stream or set it to false',
          type: 'not_implemented',
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

    // A fatal failure is the caller's problem and keeps the provider's own
    // status. Anything else means every provider was tried and none could
    // serve it, which is a gateway-level 502 regardless of what the last one
    // happened to return.
    const status = failure.disposition === 'fatal' ? (failure.status ?? 400) : 502;

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
