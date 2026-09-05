import { execFile } from 'node:child_process';
import { cpus, platform, release } from 'node:os';
import { performance } from 'node:perf_hooks';
import { promisify } from 'node:util';
import type { FastifyInstance } from 'fastify';
import pg from 'pg';
import { loadConfig, type Config } from '../src/config.js';
import { createPool } from '../src/db.js';
import { buildMockServer } from '../src/mock/server.js';
import { completionFor } from '../src/mock/tokens.js';
import { ProviderClient } from '../src/providers/client.js';
import type { ChatRequest, Usage } from '../src/providers/profile.js';
import { createRedis } from '../src/redis.js';
import { buildServer } from '../src/server.js';
import { estimatePromptTokens, reservationFor } from '../src/tokens/counter.js';

const execFileAsync = promisify(execFile);
const BENCH_DATABASE = 'modelgate_bench';
const BENCH_REDIS_DATABASE = '2';
const API_KEY = 'modelgate-benchmark-key';
const MOCK_TOKENS = 24;

function iterationCount(): number {
  const value = Number(process.env['BENCH_ITERATIONS'] ?? 200);
  if (!Number.isInteger(value) || value < 10 || value > 1_000) {
    throw new Error('BENCH_ITERATIONS must be an integer from 10 to 1000');
  }
  return value;
}

function withDatabase(raw: string, database: string): string {
  const url = new URL(raw);
  url.pathname = `/${database}`;
  return url.toString();
}

async function prepareDatabase(baseUrl: string): Promise<string> {
  const adminUrl = withDatabase(baseUrl, 'postgres');
  const benchUrl = withDatabase(baseUrl, BENCH_DATABASE);
  const admin = new pg.Client({ connectionString: adminUrl });
  await admin.connect();
  try {
    try {
      await admin.query(`CREATE DATABASE ${BENCH_DATABASE}`);
    } catch (error) {
      if ((error as { code?: string }).code !== '42P04') throw error;
    }
  } finally {
    await admin.end();
  }

  await execFileAsync(
    process.execPath,
    ['node_modules/node-pg-migrate/bin/node-pg-migrate.js', 'up'],
    { env: { ...process.env, DATABASE_URL: benchUrl } },
  );
  return benchUrl;
}

function benchRedisUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  url.pathname = `/${BENCH_REDIS_DATABASE}`;
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

function gatewayConfig(
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
    REDACTION_PEPPER: 'benchmark-pepper'.repeat(2),
    PROVIDER_ORDER: 'mock-primary,mock-backup',
    MOCK_PRIMARY_URL: primaryUrl,
    MOCK_BACKUP_URL: backupUrl,
    MAX_ATTEMPTS_PER_PROVIDER: '1',
    RETRY_BASE_MS: '0',
    RETRY_CAP_MS: '1',
    REQUEST_DEADLINE_MS: '10000',
    STREAM_COMMIT_DEADLINE_MS: '500',
    TOKEN_BUDGET_CAP: '10000000',
    TOKEN_REFILL_PER_SEC: '1000000',
    BUDGET_LEASE_TTL_MS: '10000',
    PROVIDER_HEADERS_TIMEOUT_MS: '2000',
    PROVIDER_BODY_TIMEOUT_MS: '2000',
  });
}

function percentile(values: readonly number[], fraction: number): number {
  if (values.length === 0) throw new Error('cannot take a percentile of no samples');
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.max(0, Math.ceil(sorted.length * fraction) - 1);
  return sorted[index] ?? sorted[sorted.length - 1] ?? 0;
}

function rounded(value: number): number {
  return Math.round(value * 100) / 100;
}

function summary(values: readonly number[]): { p50: number; p99: number } {
  return {
    p50: rounded(percentile(values, 0.5)),
    p99: rounded(percentile(values, 0.99)),
  };
}

function rangeSummary(values: readonly number[]): { min: number; median: number; max: number } {
  return {
    min: rounded(Math.min(...values)),
    median: rounded(percentile(values, 0.5)),
    max: rounded(Math.max(...values)),
  };
}

interface JsonTiming {
  milliseconds: number;
  response: Response;
  body: Record<string, unknown>;
}

async function timedJson(
  url: string,
  request: ChatRequest,
  authenticated: boolean,
): Promise<JsonTiming> {
  const started = performance.now();
  const response = await fetch(`${url}/v1/chat/completions`, {
    method: 'POST',
    signal: AbortSignal.timeout(10_000),
    headers: {
      'content-type': 'application/json',
      ...(authenticated ? { authorization: `Bearer ${API_KEY}` } : {}),
    },
    body: JSON.stringify(request),
  });
  const body = (await response.json()) as Record<string, unknown>;
  return { milliseconds: performance.now() - started, response, body };
}

interface StreamTiming {
  ttfbMs: number;
  totalMs: number;
  response: Response;
  text: string;
  events: string[];
  finishReason: string | null;
  sawDone: boolean;
}

async function timedStream(
  url: string,
  request: ChatRequest,
  authenticated: boolean,
): Promise<StreamTiming> {
  const started = performance.now();
  const response = await fetch(`${url}/v1/chat/completions`, {
    method: 'POST',
    signal: AbortSignal.timeout(10_000),
    headers: {
      'content-type': 'application/json',
      ...(authenticated ? { authorization: `Bearer ${API_KEY}` } : {}),
    },
    body: JSON.stringify({ ...request, stream: true }),
  });
  if (response.body === null) throw new Error('stream returned no body');

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let firstContentAt: number | null = null;
  let text = '';
  let finishReason: string | null = null;
  let sawDone = false;
  const events: string[] = [];

  const consume = (frame: string): void => {
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
      const content = choice?.delta?.content;
      if (typeof content === 'string') {
        if (firstContentAt === null) firstContentAt = performance.now();
        text += content;
      }
      if (choice?.finish_reason != null) finishReason = choice.finish_reason;
    }
  };

  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let boundary = buffer.search(/\r\n\r\n|\n\n/);
    while (boundary !== -1) {
      const separator = /\r\n\r\n|\n\n/.exec(buffer.slice(boundary))?.[0] ?? '\n\n';
      consume(buffer.slice(0, boundary));
      buffer = buffer.slice(boundary + separator.length);
      boundary = buffer.search(/\r\n\r\n|\n\n/);
    }
  }
  buffer += decoder.decode();
  if (buffer.trim() !== '') consume(buffer);

  if (firstContentAt === null) throw new Error(`stream produced no content (${response.status})`);
  return {
    ttfbMs: firstContentAt - started,
    totalMs: performance.now() - started,
    response,
    text,
    events,
    finishReason,
    sawDone,
  };
}

function chatRequest(content: string): ChatRequest {
  return {
    model: 'mock-1',
    messages: [{ role: 'user', content }],
    max_tokens: MOCK_TOKENS,
  };
}

function usageFrom(body: Record<string, unknown>): Usage {
  const usage = body['usage'];
  if (typeof usage !== 'object' || usage === null) throw new Error('provider returned no usage');
  return usage as Usage;
}

const iterations = iterationCount();
const baseDatabaseUrl = process.env['DATABASE_URL'];
const baseRedisUrl = process.env['REDIS_URL'];
if (baseDatabaseUrl === undefined || baseRedisUrl === undefined) {
  throw new Error('DATABASE_URL and REDIS_URL are required; copy .env.example to .env first');
}

const databaseUrl = await prepareDatabase(baseDatabaseUrl);
const redisUrl = benchRedisUrl(baseRedisUrl);
const db = createPool(gatewayConfig(databaseUrl, redisUrl, 'http://127.0.0.1:1', 'http://127.0.0.1:1'));
const cache = createRedis(gatewayConfig(databaseUrl, redisUrl, 'http://127.0.0.1:1', 'http://127.0.0.1:1'));
const providerClient = new ProviderClient();

const healthyPrimary = buildMockServer();
const healthyBackup = buildMockServer();
const preCommitFailure = buildMockServer({ alwaysFail: 'abort@0' });
const postCommitFailure = buildMockServer({ alwaysFail: 'abort@6' });
const gateways: FastifyInstance[] = [];
const providers = [healthyPrimary, healthyBackup, preCommitFailure, postCommitFailure];

try {
  await waitForRedis(cache);
  await cache.flushdb();
  await db.query('TRUNCATE requests, request_payloads, request_attempts, idempotency_keys');

  const healthyPrimaryUrl = await listen(healthyPrimary);
  const healthyBackupUrl = await listen(healthyBackup);
  const preCommitFailureUrl = await listen(preCommitFailure);
  const postCommitFailureUrl = await listen(postCommitFailure);

  const healthyGateway = buildServer({
    config: gatewayConfig(databaseUrl, redisUrl, healthyPrimaryUrl, healthyBackupUrl),
    db,
    cache,
    client: providerClient,
  });
  const failoverGateway = buildServer({
    config: gatewayConfig(databaseUrl, redisUrl, preCommitFailureUrl, healthyBackupUrl),
    db,
    cache,
    client: providerClient,
  });
  const interruptedGateway = buildServer({
    config: gatewayConfig(databaseUrl, redisUrl, postCommitFailureUrl, healthyBackupUrl),
    db,
    cache,
    client: providerClient,
  });
  gateways.push(healthyGateway, failoverGateway, interruptedGateway);

  const healthyGatewayUrl = await listen(healthyGateway);
  const failoverGatewayUrl = await listen(failoverGateway);
  const interruptedGatewayUrl = await listen(interruptedGateway);

  await timedJson(healthyPrimaryUrl, chatRequest('warm direct'), false);
  await timedJson(healthyGatewayUrl, chatRequest('warm gateway'), true);
  await timedStream(healthyGatewayUrl, chatRequest('warm stream'), true);
  await timedStream(failoverGatewayUrl, chatRequest('warm failover'), true);

  const directMs: number[] = [];
  const coldGatewayMs: number[] = [];
  const addedMs: number[] = [];
  for (let index = 0; index < iterations; index += 1) {
    const request = chatRequest(`cold benchmark ${index}`);
    const direct = await timedJson(healthyPrimaryUrl, request, false);
    const gateway = await timedJson(healthyGatewayUrl, request, true);
    if (direct.response.status !== 200 || gateway.response.status !== 200) {
      throw new Error('cold latency sample failed');
    }
    if (gateway.response.headers.get('x-modelgate-cache') !== 'miss') {
      throw new Error('cold latency sample unexpectedly hit the cache');
    }
    directMs.push(direct.milliseconds);
    coldGatewayMs.push(gateway.milliseconds);
    addedMs.push(gateway.milliseconds - direct.milliseconds);
  }

  const cacheRequest = chatRequest('fixed cache benchmark');
  await timedJson(healthyGatewayUrl, cacheRequest, true);
  const cacheHitMs: number[] = [];
  for (let index = 0; index < iterations; index += 1) {
    const hit = await timedJson(healthyGatewayUrl, cacheRequest, true);
    if (hit.response.headers.get('x-modelgate-cache') !== 'hit') {
      throw new Error('warmed request missed the cache');
    }
    cacheHitMs.push(hit.milliseconds);
  }

  const streamIterations = Math.max(10, Math.floor(iterations / 2));
  const directTtftMs: number[] = [];
  const gatewayTtftMs: number[] = [];
  const failoverTtftMs: number[] = [];
  let preCommitOutputMismatches = 0;
  const expected = completionFor(1, MOCK_TOKENS);
  for (let index = 0; index < streamIterations; index += 1) {
    const request = chatRequest(`stream benchmark ${index}`);
    const direct = await timedStream(healthyPrimaryUrl, request, false);
    const gateway = await timedStream(healthyGatewayUrl, request, true);
    const failover = await timedStream(failoverGatewayUrl, request, true);
    if (failover.response.headers.get('x-modelgate-provider') !== 'mock-backup') {
      throw new Error('pre-commit failure did not reach the backup');
    }
    if (failover.text !== expected) preCommitOutputMismatches += 1;
    directTtftMs.push(direct.ttfbMs);
    gatewayTtftMs.push(gateway.ttfbMs);
    failoverTtftMs.push(failover.ttfbMs);
  }

  const interruptionSamples = Math.max(10, Math.floor(streamIterations / 2));
  let silentTruncations = 0;
  for (let index = 0; index < interruptionSamples; index += 1) {
    const interrupted = await timedStream(
      interruptedGatewayUrl,
      chatRequest(`interrupted benchmark ${index}`),
      true,
    );
    const explicit =
      interrupted.events.includes('error') &&
      interrupted.finishReason === 'modelgate_interrupted' &&
      interrupted.sawDone;
    if (!explicit) silentTruncations += 1;
  }

  const accountingPrompts = [
    'hello',
    'explain why token boundaries do not follow stream chunks',
    'emoji 🙂🙂🙂 and 日本語 across boundaries',
    'a'.repeat(127),
    'one two three four five six seven eight nine ten',
  ];
  const promptErrors: number[] = [];
  const reservationRatios: number[] = [];
  for (const content of accountingPrompts) {
    const request = chatRequest(content);
    const direct = await timedJson(healthyPrimaryUrl, request, false);
    const usage = usageFrom(direct.body);
    const promptEstimate = estimatePromptTokens(request);
    const reservation = reservationFor(request).reserve;
    promptErrors.push(Math.abs(promptEstimate - usage.prompt_tokens) / usage.prompt_tokens);
    reservationRatios.push(usage.total_tokens / reservation);
  }

  const result = {
    recordedAt: new Date().toISOString(),
    environment: {
      node: process.version,
      platform: `${platform()} ${release()}`,
      cpu: cpus()[0]?.model ?? 'unknown',
    },
    samples: {
      requestLatency: iterations,
      streamTtft: streamIterations,
      interruptions: interruptionSamples,
      accountingPrompts: accountingPrompts.length,
    },
    latencyMs: {
      directProvider: summary(directMs),
      gatewayCold: summary(coldGatewayMs),
      gatewayAdded: summary(addedMs),
      gatewayCacheHit: summary(cacheHitMs),
    },
    streamingTtftMs: {
      directProvider: summary(directTtftMs),
      healthyGateway: summary(gatewayTtftMs),
      preCommitFailover: summary(failoverTtftMs),
    },
    streamGuarantees: {
      preCommitOutputMismatches,
      silentTruncations,
    },
    accountingAgainstMock: {
      promptEstimateAbsoluteErrorPercent: rangeSummary(promptErrors.map((value) => value * 100)),
      actualToReservedPercent: rangeSummary(reservationRatios.map((value) => value * 100)),
    },
  };

  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} finally {
  await Promise.all(gateways.map((app) => app.close()));
  await providerClient.close();
  await Promise.all(providers.map((app) => app.close()));
  await db.query('TRUNCATE requests, request_payloads, request_attempts, idempotency_keys');
  if (cache.status === 'ready') await cache.flushdb();
  await cache.quit();
  await db.end();
}
