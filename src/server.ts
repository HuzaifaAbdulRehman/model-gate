import Fastify, { type FastifyInstance } from 'fastify';
import type { Config } from './config.js';
import type { Db } from './db.js';
import type { Cache } from './redis.js';

export interface ServerDeps {
  config: Config;
  db: Db;
  cache: Cache;
}

export function buildServer({ config, db, cache }: ServerDeps): FastifyInstance {
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

  return app;
}
