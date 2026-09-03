import type { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { z } from 'zod';
import type { ProviderClient } from '../providers/client.js';
import type { ChatRequest, ProviderProfile } from '../providers/profile.js';
import { apiKeyMatches, bearerToken } from './auth.js';
import { dispatch } from './dispatch.js';
import type { BackoffOptions } from './backoff.js';

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

    // Once the caller has hung up there is nobody to answer, and continuing
    // through the chain spends provider calls on a response that will be
    // discarded. `close` also fires on a normal finish, so the guard is whether
    // the response actually completed.
    const controller = new AbortController();
    const onClose = (): void => {
      if (!reply.raw.writableFinished) controller.abort();
    };
    reply.raw.on('close', onClose);

    let result;
    try {
      result = await dispatch(parsed.data as ChatRequest, {
        providers: deps.providers,
        client: deps.client,
        apiKeys: deps.apiKeys,
        maxAttemptsPerProvider: deps.maxAttemptsPerProvider,
        backoff: deps.backoff,
        signal: controller.signal,
      });
    } finally {
      reply.raw.off('close', onClose);
    }

    // Named so a demo can show which provider actually answered, and so a
    // failover is visible without reading the audit table.
    void reply.header('x-modelgate-attempts', String(result.attempts.length));

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
