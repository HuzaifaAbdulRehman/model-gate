import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import type { FastifyInstance } from 'fastify';
import pg from 'pg';
import { loadConfig, type Config } from '../src/config.js';
import { createPool } from '../src/db.js';
import { buildMockServer } from '../src/mock/server.js';
import { completionFor } from '../src/mock/tokens.js';
import { ProviderClient } from '../src/providers/client.js';
import { createRedis } from '../src/redis.js';
import { buildServer } from '../src/server.js';

const execFileAsync = promisify(execFile);
const DATABASE = 'modelgate_demo';
const REDIS_DATABASE = '3';
const API_KEY = 'modelgate-demo-key';

function withDatabase(raw: string, database: string): string {
  const url = new URL(raw);
  url.pathname = `/${database}`;
  return url.toString();
}

async function prepareDatabase(baseUrl: string): Promise<string> {
  const databaseUrl = withDatabase(baseUrl, DATABASE);
  const admin = new pg.Client({ connectionString: withDatabase(baseUrl, 'postgres') });
  await admin.connect();
  try {
    try {
      await admin.query(`CREATE DATABASE ${DATABASE}`);
    } catch (error) {
      if ((error as { code?: string }).code !== '42P04') throw error;
    }
  } finally {
    await admin.end();
  }
  await execFileAsync(
    process.execPath,
    ['node_modules/node-pg-migrate/bin/node-pg-migrate.js', 'up'],
    { env: { ...process.env, DATABASE_URL: databaseUrl } },
  );
  return databaseUrl;
}

function redisDatabase(raw: string): string {
  const url = new URL(raw);
  url.pathname = `/${REDIS_DATABASE}`;
  return url.toString();
}

function waitForRedis(cache: ReturnType<typeof createRedis>): Promise<void> {
  if (cache.status === 'ready') return Promise.resolve();
  return new Promise((resolve, reject) => {
    cache.once('ready', resolve);
    cache.once('error', reject);
  });
}

async function listen(app: FastifyInstance): Promise<string> {
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  if (address === null || typeof address === 'string') throw new Error('server did not bind a port');
  return `http://127.0.0.1:${address.port}`;
}

function config(
  databaseUrl: string,
  redisUrl: string,
  primaryUrl: string,
  backupUrl: string,
): Config {
  return loadConfig({
    ...process.env,
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    DATABASE_URL: databaseUrl,
    REDIS_URL: redisUrl,
    GATEWAY_API_KEY: API_KEY,
    REDACTION_PEPPER: 'demo-redaction-pepper'.repeat(2),
    PROVIDER_ORDER: 'mock-primary,mock-backup',
    MOCK_PRIMARY_URL: primaryUrl,
    MOCK_BACKUP_URL: backupUrl,
    MAX_ATTEMPTS_PER_PROVIDER: '1',
    RETRY_BASE_MS: '0',
    RETRY_CAP_MS: '1',
    TOKEN_BUDGET_CAP: '100000',
    TOKEN_REFILL_PER_SEC: '100000',
    BUDGET_LEASE_TTL_MS: '10000',
    PROVIDER_HEADERS_TIMEOUT_MS: '2000',
    PROVIDER_BODY_TIMEOUT_MS: '2000',
  });
}

interface Collected {
  status: number;
  provider: string | null;
  text: string;
  events: string[];
  finishReason: string | null;
  sawDone: boolean;
}

async function collect(url: string): Promise<Collected> {
  const response = await fetch(`${url}/v1/chat/completions`, {
    method: 'POST',
    signal: AbortSignal.timeout(10_000),
    headers: {
      authorization: `Bearer ${API_KEY}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      model: 'mock-1',
      messages: [{ role: 'user', content: 'show the failover boundary' }],
      max_tokens: 24,
      stream: true,
    }),
  });
  const raw = await response.text();
  let text = '';
  let finishReason: string | null = null;
  let sawDone = false;
  const events: string[] = [];

  for (const frame of raw.split(/\r\n\r\n|\n\n/)) {
    for (const line of frame.split(/\r\n|\n/)) {
      if (line.startsWith('event:')) {
        events.push(line.slice(6).trim());
        continue;
      }
      if (!line.startsWith('data:')) continue;
      const payload = line.slice(5).trim();
      if (payload === '[DONE]') {
        sawDone = true;
        continue;
      }
      let chunk: {
        choices?: Array<{ delta?: { content?: string }; finish_reason?: string | null }>;
      };
      try {
        chunk = JSON.parse(payload) as typeof chunk;
      } catch {
        continue;
      }
      const choice = chunk.choices?.[0];
      if (typeof choice?.delta?.content === 'string') text += choice.delta.content;
      if (choice?.finish_reason != null) finishReason = choice.finish_reason;
    }
  }

  return {
    status: response.status,
    provider: response.headers.get('x-modelgate-provider'),
    text,
    events,
    finishReason,
    sawDone,
  };
}

const baseDatabaseUrl = process.env['DATABASE_URL'];
const baseRedisUrl = process.env['REDIS_URL'];
if (baseDatabaseUrl === undefined || baseRedisUrl === undefined) {
  throw new Error('DATABASE_URL and REDIS_URL are required; copy .env.example to .env first');
}

const databaseUrl = await prepareDatabase(baseDatabaseUrl);
const redisUrl = redisDatabase(baseRedisUrl);
const sharedConfig = config(databaseUrl, redisUrl, 'http://127.0.0.1:1', 'http://127.0.0.1:1');
const db = createPool(sharedConfig);
const cache = createRedis(sharedConfig);
const client = new ProviderClient();
const backup = buildMockServer();
const preCommitFailure = buildMockServer({ alwaysFail: 'abort@0' });
const postCommitFailure = buildMockServer({ alwaysFail: 'abort@6' });
const providers = [backup, preCommitFailure, postCommitFailure];
const gateways: FastifyInstance[] = [];

try {
  await waitForRedis(cache);
  await cache.flushdb();
  await db.query('TRUNCATE requests, request_payloads, request_attempts, idempotency_keys');

  const backupUrl = await listen(backup);
  const preCommitFailureUrl = await listen(preCommitFailure);
  const postCommitFailureUrl = await listen(postCommitFailure);
  const preCommitGateway = buildServer({
    config: config(databaseUrl, redisUrl, preCommitFailureUrl, backupUrl),
    db,
    cache,
    client,
  });
  const postCommitGateway = buildServer({
    config: config(databaseUrl, redisUrl, postCommitFailureUrl, backupUrl),
    db,
    cache,
    client,
  });
  gateways.push(preCommitGateway, postCommitGateway);

  const preCommit = await collect(await listen(preCommitGateway));
  const postCommit = await collect(await listen(postCommitGateway));
  const expected = completionFor(1, 24);

  const result = {
    preCommitFailure: {
      status: preCommit.status,
      provider: preCommit.provider,
      exactBackupOutput: preCommit.text === expected,
      finishReason: preCommit.finishReason,
      sawDone: preCommit.sawDone,
    },
    postCommitFailure: {
      status: postCommit.status,
      provider: postCommit.provider,
      partialCharacters: postCommit.text.length,
      explicitError: postCommit.events.includes('error'),
      finishReason: postCommit.finishReason,
      sawDone: postCommit.sawDone,
    },
  };

  if (
    preCommit.status !== 200 ||
    preCommit.provider !== 'mock-backup' ||
    preCommit.text !== expected ||
    preCommit.finishReason !== 'stop' ||
    !preCommit.sawDone ||
    postCommit.status !== 200 ||
    postCommit.provider !== 'mock-primary' ||
    !postCommit.events.includes('error') ||
    postCommit.finishReason !== 'modelgate_interrupted' ||
    !postCommit.sawDone
  ) {
    throw new Error(`demo guarantee failed:\n${JSON.stringify(result, null, 2)}`);
  }

  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} finally {
  await Promise.all(gateways.map((app) => app.close()));
  await client.close();
  await Promise.all(providers.map((app) => app.close()));
  await db.query('TRUNCATE requests, request_payloads, request_attempts, idempotency_keys');
  if (cache.status === 'ready') await cache.flushdb();
  await cache.quit();
  await db.end();
}
