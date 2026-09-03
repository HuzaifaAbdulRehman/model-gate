import { randomUUID } from 'node:crypto';
import type { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { z } from 'zod';
import type { TokenBudget } from '../limits/budget.js';
import type { ProviderClient } from '../providers/client.js';
import type { ChatRequest, ProviderProfile } from '../providers/profile.js';
import { countTokens, reservationFor } from '../tokens/counter.js';
import { apiKeyMatches, bearerToken } from './auth.js';
import { dispatch, type DispatchResult } from './dispatch.js';
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
): { tokens: number; source: 'provider' | 'estimated' } {
  const usage = result.success?.usage;
  if (usage?.total_tokens !== undefined) {
    return { tokens: usage.total_tokens, source: 'provider' };
  }

  if (result.success !== null) {
    const choices = result.success.body['choices'];
    const content = Array.isArray(choices)
      ? (choices[0] as { message?: { content?: unknown } } | undefined)?.message?.content
      : undefined;
    const completion = typeof content === 'string' ? countTokens(content) : 0;
    return { tokens: estimatedPrompt + completion, source: 'estimated' };
  }

  // Nothing was served. Failed attempts did cost the provider something, but no
  // number for it exists anywhere, so the reservation is returned in full. That
  // errs in the tenant's favour, which is the safer direction to be wrong in.
  return { tokens: 0, source: 'estimated' };
}

export const gatewayRoutes: FastifyPluginAsync<GatewayDeps> = async (
  app: FastifyInstance,
  deps: GatewayDeps,
) => {
  app.addHook('onRequest', async (request, reply) => {
    const token = bearerToken(request.headers.authorization);
    if (token === null || !apiKeyMatches(token, deps.apiKey)) {
      return reply.code(401).send({
        error: { message: 'invalid or missing api key', type: 'authentication_error' },
      });
    }
  });

  app.post('/v1/chat/completions', async (request, reply) => {
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
    const { prompt, reserve } = reservationFor(chatRequest);

    // The real cost is unknown until the response ends, so the worst case is
    // held first and the difference given back afterwards. Checking afterwards
    // instead would admit every concurrent request on the same stale reading.
    const gate = await deps.budget.reserve(deps.tenantId, MODEL_CLASS, requestId, reserve);

    if (gate.impossible) {
      // A different answer from "not right now". No amount of waiting makes a
      // request larger than the whole bucket fit, so a 429 would be telling the
      // caller to retry something that can never succeed.
      return reply.code(400).send({
        error: {
          message: `this request reserves about ${reserve} tokens, which is more than the budget can ever hold`,
          type: 'invalid_request_error',
          code: 'request_exceeds_budget',
        },
      });
    }

    if (!gate.admitted) {
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
    let tokenSource: 'provider' | 'estimated' = 'estimated';
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
      const used = result === null ? { tokens: 0, source: 'estimated' as const } : actualUsage(result, prompt);
      tokenSource = used.source;
      remaining = (
        await deps.budget.settle(deps.tenantId, MODEL_CLASS, requestId, reserve, used.tokens)
      ).remaining;
    }

    // Named so a demo can show which provider actually answered, and so a
    // failover is visible without reading the audit table.
    void reply.header('x-modelgate-attempts', String(result.attempts.length));
    void reply.header('x-modelgate-tokens-remaining', String(remaining));
    void reply.header('x-modelgate-token-source', tokenSource);

    if (result.success !== null) {
      return reply
        .header('x-modelgate-provider', result.success.provider)
        .code(200)
        .send(result.success.body);
    }

    const failure = result.failure;
    if (failure === null) {
      return reply.code(502).send({
        error: { message: 'no providers are configured', type: 'configuration_error' },
      });
    }

    // A fatal failure is the caller's problem and keeps the provider's own
    // status. Anything else means every provider was tried and none could
    // serve it, which is a gateway-level 502 regardless of what the last one
    // happened to return.
    const status = failure.disposition === 'fatal' ? (failure.status ?? 400) : 502;

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
