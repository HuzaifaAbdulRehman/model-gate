import { buildMockServer } from './server.js';

// Deliberately no .env and no database. The mock is a provider stand-in, and it
// has to be startable on its own for a demo or a manual curl.
const port = Number(process.env['MOCK_PORT'] ?? 3200);
const host = process.env['MOCK_HOST'] ?? '127.0.0.1';

const app = buildMockServer({
  logLevel: process.env['LOG_LEVEL'] ?? 'info',
  dialect: process.env['MOCK_DIALECT'] === 'groq' ? 'groq' : 'openai',
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    app.close().then(
      () => process.exit(0),
      () => process.exit(1),
    );
  });
}

try {
  await app.listen({ port, host });
} catch (err) {
  app.log.error(err);
  process.exit(1);
}
