import { loadConfig } from './config.js';
import { createPool } from './db.js';
import { ProviderClient } from './providers/client.js';
import { createRedis } from './redis.js';
import { buildServer } from './server.js';

const config = loadConfig();
const db = createPool(config);
const cache = createRedis(config);
const client = new ProviderClient();

const app = buildServer({ config, db, cache, client });

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    app.log.info({ signal }, 'shutting down');
    // The server closes first so in-flight requests finish before the pools and
    // connections they need are torn down.
    Promise.resolve()
      .then(() => app.close())
      .then(() => client.close())
      .then(() => cache.quit())
      .then(() => db.end())
      .then(
        () => process.exit(0),
        (err: unknown) => {
          app.log.error(err);
          process.exit(1);
        },
      );
  });
}

app.log.info({ providers: config.PROVIDER_ORDER }, 'provider chain');

try {
  await app.listen({ port: config.PORT, host: config.HOST });
} catch (err) {
  app.log.error(err);
  process.exit(1);
}
