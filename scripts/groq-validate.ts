import { execFile } from 'node:child_process';
import { cpus, platform, release } from 'node:os';
import { promisify } from 'node:util';
import type { FastifyInstance } from 'fastify';
import pg from 'pg';
import { loadConfig } from '../src/config.js';
import { createPool } from '../src/db.js';
import { ProviderClient } from '../src/providers/client.js';
import type { ChatRequest, Usage } from '../src/providers/profile.js';
import { createRedis } from '../src/redis.js';
import { buildServer } from '../src/server.js';
import { estimatePromptTokens } from '../src/tokens/counter.js';

const execFileAsync = promisify(execFile);
const DATABASE = 'modelgate_groq_validation';
const REDIS_DATABASE = '4';
const GATEWAY_KEY = 'modelgate-groq-validation-key';
const MODEL = process.env['GROQ_MODEL'] ?? 'openai/gpt-oss-20b';
const PREFILL_RUNS = 20;
const TARGET = 'The brass compass pointed north while rain tapped the cabin window.';
const PREFILL = 'The brass compass pointed nor';
const EXPECTED_SUFFIX = TARGET.slice(PREFILL.length);

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

function validationRedisUrl(raw: string): string {
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

function usageFrom(value: unknown): Usage | null {
  if (typeof value !== 'object' || value === null) return null;
  const body = value as { usage?: Usage; x_groq?: { usage?: Usage } };
  return body.usage ?? body.x_groq?.usage ?? null;
}

function responseContent(body: Record<string, unknown>): string {
  const choices = body['choices'];
  if (!Array.isArray(choices)) return '';
  const content = (choices[0] as { message?: { content?: unknown } } | undefined)?.message?.content;
  return typeof content === 'string' ? content : '';
}

async function postJson(
  url: string,
  request: ChatRequest,
): Promise<{ body: Record<string, unknown>; provider: string | null }> {
  const response = await fetch(`${url}/v1/chat/completions`, {
    method: 'POST',
    signal: AbortSignal.timeout(60_000),
    headers: {
      authorization: `Bearer ${GATEWAY_KEY}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify(request),
  });
  const raw = await response.text();
  let body: Record<string, unknown>;
  try {
    body = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    throw new Error(`Groq returned non-JSON with HTTP ${response.status}: ${raw.slice(0, 200)}`);
  }
  if (!response.ok) {
    throw new Error(`Groq validation failed with HTTP ${response.status}: ${raw.slice(0, 500)}`);
  }
  return { body, provider: response.headers.get('x-modelgate-provider') };
}

interface StreamResult {
  provider: string | null;
  text: string;
  finishReason: string | null;
  sawDone: boolean;
  usage: Usage | null;
}

async function postStream(url: string, request: ChatRequest): Promise<StreamResult> {
  const response = await fetch(`${url}/v1/chat/completions`, {
    method: 'POST',
    signal: AbortSignal.timeout(60_000),
    headers: {
      authorization: `Bearer ${GATEWAY_KEY}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ ...request, stream: true }),
  });
  const raw = await response.text();
  if (!response.ok) {
    throw new Error(`Groq stream failed with HTTP ${response.status}: ${raw.slice(0, 500)}`);
  }

  let text = '';
  let finishReason: string | null = null;
  let sawDone = false;
  let usage: Usage | null = null;
  for (const frame of raw.split(/\r\n\r\n|\n\n/)) {
    for (const line of frame.split(/\r\n|\n/)) {
      if (!line.startsWith('data:')) continue;
      const payload = line.slice(5).trim();
      if (payload === '[DONE]') {
        sawDone = true;
        continue;
      }

      let chunk: {
        choices?: Array<{ delta?: { content?: string }; finish_reason?: string | null }>;
        usage?: Usage;
        x_groq?: { usage?: Usage };
      };
      try {
        chunk = JSON.parse(payload) as typeof chunk;
      } catch {
        continue;
      }
      const choice = chunk.choices?.[0];
      if (typeof choice?.delta?.content === 'string') text += choice.delta.content;
      if (choice?.finish_reason != null) finishReason = choice.finish_reason;
      usage = usageFrom(chunk) ?? usage;
    }
  }

  return {
    provider: response.headers.get('x-modelgate-provider'),
    text,
    finishReason,
    sawDone,
    usage,
  };
}

function range(values: readonly number[]): { min: number; median: number; max: number } {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = sorted[Math.floor(sorted.length / 2)] ?? 0;
  return {
    min: sorted[0] ?? 0,
    median: middle,
    max: sorted[sorted.length - 1] ?? 0,
  };
}

function rounded(value: number): number {
  return Math.round(value * 100) / 100;
}

type PrefillOutcome =
  | 'exact_suffix'
  | 'exact_full_response'
  | 'replayed_prefix_other'
  | 'other';

function classifyPrefill(content: string): PrefillOutcome {
  const trimmed = content.trim();
  if (trimmed === EXPECTED_SUFFIX) return 'exact_suffix';
  if (trimmed === TARGET) return 'exact_full_response';
  if (trimmed.startsWith(PREFILL)) return 'replayed_prefix_other';
  return 'other';
}

const baseDatabaseUrl = process.env['DATABASE_URL'];
const baseRedisUrl = process.env['REDIS_URL'];
const groqKey = process.env['GROQ_API_KEY'];
if (baseDatabaseUrl === undefined || baseRedisUrl === undefined) {
  throw new Error('DATABASE_URL and REDIS_URL are required; copy .env.example to .env first');
}
if (groqKey === undefined || groqKey.trim() === '') {
  throw new Error('GROQ_API_KEY is required for live validation');
}

const databaseUrl = await prepareDatabase(baseDatabaseUrl);
const redisUrl = validationRedisUrl(baseRedisUrl);
const config = loadConfig({
  ...process.env,
  NODE_ENV: 'test',
  LOG_LEVEL: 'silent',
  DATABASE_URL: databaseUrl,
  REDIS_URL: redisUrl,
  GATEWAY_API_KEY: GATEWAY_KEY,
  REDACTION_PEPPER: 'groq-validation-pepper'.repeat(2),
  PROVIDER_ORDER: 'groq',
  GROQ_API_KEY: groqKey,
  MAX_ATTEMPTS_PER_PROVIDER: '1',
  RETRY_BASE_MS: '0',
  RETRY_CAP_MS: '1',
  REQUEST_DEADLINE_MS: '60000',
  STREAM_COMMIT_DEADLINE_MS: '1000',
  CACHE_ENABLED: 'false',
  TOKEN_BUDGET_CAP: '1000000',
  TOKEN_REFILL_PER_SEC: '100000',
  BUDGET_LEASE_TTL_MS: '120000',
  PROVIDER_HEADERS_TIMEOUT_MS: '30000',
  PROVIDER_BODY_TIMEOUT_MS: '60000',
});
const db = createPool(config);
const cache = createRedis(config);
const client = new ProviderClient();
const app = buildServer({ config, db, cache, client });

try {
  await waitForRedis(cache);
  await cache.flushdb();
  await db.query('TRUNCATE requests, request_payloads, request_attempts, idempotency_keys');
  const url = await listen(app);

  const calibrationRequests: Array<{ name: string; request: ChatRequest }> = [
    {
      name: 'one-message',
      request: {
        model: MODEL,
        messages: [{ role: 'user', content: 'Reply with the single word ready.' }],
        max_tokens: 32,
        reasoning_effort: 'low',
      },
    },
    {
      name: 'system-and-user',
      request: {
        model: MODEL,
        messages: [
          { role: 'system', content: 'Answer briefly.' },
          { role: 'user', content: 'Return the number seven.' },
        ],
        max_tokens: 32,
        reasoning_effort: 'low',
      },
    },
    {
      name: 'unicode',
      request: {
        model: MODEL,
        messages: [
          {
            role: 'user',
            content: '\u65e5\u672c\u8a9e and three emoji \ud83d\ude42\ud83d\ude42\ud83d\ude42',
          },
        ],
        max_tokens: 32,
        reasoning_effort: 'low',
      },
    },
    {
      name: 'three-message-conversation',
      request: {
        model: MODEL,
        messages: [
          { role: 'user', content: 'Remember the code word amber.' },
          { role: 'assistant', content: 'I will remember amber.' },
          { role: 'user', content: 'What was the code word?' },
        ],
        max_tokens: 32,
        reasoning_effort: 'low',
      },
    },
    {
      name: 'long-unbroken-text',
      request: {
        model: MODEL,
        messages: [{ role: 'user', content: 'a'.repeat(127) }],
        max_tokens: 32,
        reasoning_effort: 'low',
      },
    },
  ];

  const tokenDifferences: number[] = [];
  const tokenErrorsPercent: number[] = [];
  const calibrationSamples: Array<{
    name: string;
    estimated: number;
    provider: number;
    estimatedMinusProvider: number;
    absoluteErrorPercent: number;
  }> = [];
  for (const { name, request } of calibrationRequests) {
    const response = await postJson(url, request);
    if (response.provider !== 'groq') throw new Error('calibration request did not use Groq');
    const usage = usageFrom(response.body);
    if (usage === null) throw new Error('Groq calibration response carried no usage');
    const estimated = estimatePromptTokens(request);
    const difference = estimated - usage.prompt_tokens;
    const errorPercent = (Math.abs(difference) / usage.prompt_tokens) * 100;
    tokenDifferences.push(difference);
    tokenErrorsPercent.push(errorPercent);
    calibrationSamples.push({
      name,
      estimated,
      provider: usage.prompt_tokens,
      estimatedMinusProvider: difference,
      absoluteErrorPercent: rounded(errorPercent),
    });
  }

  const outcomes: Record<PrefillOutcome, number> = {
    exact_suffix: 0,
    exact_full_response: 0,
    replayed_prefix_other: 0,
    other: 0,
  };
  const sampleOutputs = new Set<string>();
  const prefillRequest: ChatRequest = {
    model: MODEL,
    messages: [
      { role: 'system', content: 'Follow the exact-output instruction. Do not explain.' },
      { role: 'user', content: `Reply with exactly this sentence: ${TARGET}` },
      { role: 'assistant', content: PREFILL },
    ],
    temperature: 0,
    max_tokens: 64,
    reasoning_effort: 'low',
  };

  for (let index = 0; index < PREFILL_RUNS; index += 1) {
    const response = await postJson(url, prefillRequest);
    if (response.provider !== 'groq') throw new Error('prefill request did not use Groq');
    const content = responseContent(response.body);
    outcomes[classifyPrefill(content)] += 1;
    if (sampleOutputs.size < 3) sampleOutputs.add(content.slice(0, 160));
  }

  const stream = await postStream(url, {
    model: MODEL,
    messages: [{ role: 'user', content: 'Reply with exactly: MODEL GATE LIVE' }],
    temperature: 0,
    max_tokens: 64,
    reasoning_effort: 'low',
  });
  if (
    stream.provider !== 'groq' ||
    stream.text.length === 0 ||
    stream.finishReason !== 'stop' ||
    !stream.sawDone ||
    stream.usage === null
  ) {
    throw new Error(`live Groq stream failed validation: ${JSON.stringify(stream)}`);
  }

  const expectedAuditRows = calibrationRequests.length + PREFILL_RUNS + 1;
  let auditRows = 0;
  let providerUsageRows = 0;
  for (let attempt = 0; attempt < 40; attempt += 1) {
    const result = await db.query<{ count: string; provider_count: string }>(
      `SELECT count(*)::text AS count,
              count(*) FILTER (WHERE token_source = 'provider')::text AS provider_count
         FROM requests`,
    );
    auditRows = Number(result.rows[0]?.count ?? 0);
    providerUsageRows = Number(result.rows[0]?.provider_count ?? 0);
    if (auditRows === expectedAuditRows) break;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  if (auditRows !== expectedAuditRows || providerUsageRows !== expectedAuditRows) {
    throw new Error(
      `expected ${expectedAuditRows} provider-backed audit rows, found ${auditRows} rows and ${providerUsageRows} provider counts`,
    );
  }

  const result = {
    recordedAt: new Date().toISOString(),
    environment: {
      node: process.version,
      platform: `${platform()} ${release()}`,
      cpu: cpus()[0]?.model ?? 'unknown',
    },
    model: MODEL,
    requests: expectedAuditRows,
    gateway: {
      nonStreaming: 'ok',
      streaming: {
        finishReason: stream.finishReason,
        sawDone: stream.sawDone,
        providerUsage: stream.usage !== null,
      },
      providerBackedAuditRows: providerUsageRows,
    },
    promptAccounting: {
      samples: calibrationRequests.length,
      calibrationSamples,
      estimatedMinusProviderTokens: range(tokenDifferences),
      absoluteErrorPercent: Object.fromEntries(
        Object.entries(range(tokenErrorsPercent)).map(([key, value]) => [key, rounded(value)]),
      ),
    },
    prefill: {
      runs: PREFILL_RUNS,
      prefixEndsMidWord: true,
      outcomes,
      sampleOutputs: [...sampleOutputs],
    },
  };

  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} finally {
  await app.close();
  await client.close();
  await db.query('TRUNCATE requests, request_payloads, request_attempts, idempotency_keys');
  if (cache.status === 'ready') await cache.flushdb();
  await cache.quit();
  await db.end();
}
