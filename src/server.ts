import Fastify, { type FastifyInstance } from 'fastify';
import type { Config } from './config.js';
import type { Db } from './db.js';
import { gatewayRoutes } from './gateway/routes.js';
import type { ProviderClient } from './providers/client.js';
import { buildApiKeys, buildProviderChain } from './providers/registry.js';
import type { Cache } from './redis.js';

export interface ServerDeps {
  config: Config;
  db: Db;
  cache: Cache;
  client: ProviderClient;
}

export function buildServer({ config, db, cache, client }: ServerDeps): FastifyInstance {
  const app = Fastify({
    logger: { level: config.LOG_LEVEL },
  });

  // Liveness. Answers as long as the process is up, so a dead dependency does
  // not get the container restarted while it is still the dependency that is
  // broken.
  app.get('/health', async () => ({
    status: 'ok',
    uptime: Math.floor(process.uptime()),
  }));

  // Readiness. Both dependencies are checked because the gateway cannot do its
  // job without either: no Postgres means no audit trail, and no Redis means no
  // token budget. Returning 200 here while one is down would be a lie the load
  // balancer acts on.
  app.get('/ready', async (_request, reply) => {
    const [postgres, redis] = await Promise.all([
      db
        .query('SELECT 1')
        .then(() => true)
        .catch(() => false),
      cache
        .ping()
        .then((pong) => pong === 'PONG')
        .catch(() => false),
    ]);

    const ready = postgres && redis;
    return reply.code(ready ? 200 : 503).send({ ready, postgres, redis });
  });

  // Registered as its own plugin so the bearer-token hook stays scoped to the
  // gateway routes and never runs against the health probes, which have to
  // answer an unauthenticated load balancer.
  void app.register(gatewayRoutes, {
    apiKey: config.GATEWAY_API_KEY,
    providers: buildProviderChain(config),
    apiKeys: buildApiKeys(config),
    client,
    maxAttemptsPerProvider: config.MAX_ATTEMPTS_PER_PROVIDER,
    backoff: { baseMs: config.RETRY_BASE_MS, capMs: config.RETRY_CAP_MS },
  });

  return app;
}
