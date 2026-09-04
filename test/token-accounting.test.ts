import type { FastifyInstance } from 'fastify';
import type pg from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';
import { contentPieces } from '../src/mock/tokens.js';
import { ProviderClient } from '../src/providers/client.js';
import type { Cache } from '../src/redis.js';
import { buildServer } from '../src/server.js';
import { countTokens } from '../src/tokens/counter.js';
import { createTestPool, createTestRedis, truncateAll, waitForRedis } from './helpers/db.js';
import { TEST_DATABASE_URL } from './helpers/global-setup.js';
import { readSse, startMock, type Collected, type RunningMock } from './helpers/mock.js';

const db: pg.Pool = createTestPool();
const cache: Cache = createTestRedis();
const API_KEY = 'x'.repeat(24);
const TENANT = 'default';

interface Harness {
  url: string;
  provider: RunningMock;
  close: () => Promise<void>;
}

let open: Harness | null = null;

async function harness(
  options: {
    fails?: string;
    dialect?: 'openai' | 'groq';
    splitDeltas?: boolean;
    env?: Record<string, string>;
  } = {},
): Promise<Harness> {
  const provider = await startMock({
    ...(options.fails !== undefined ? { alwaysFail: options.fails } : {}),
    ...(options.dialect !== undefined ? { dialect: options.dialect } : {}),
    ...(options.splitDeltas === true ? { splitDeltas: true } : {}),
  });
  const config = loadConfig({
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    DATABASE_URL: TEST_DATABASE_URL,
    REDIS_URL: 'redis://localhost:6380',
    GATEWAY_API_KEY: API_KEY,
    REDACTION_PEPPER: 'p'.repeat(32),
    MOCK_PRIMARY_URL: provider.url,
    MOCK_BACKUP_URL: provider.url,
    ...options.env,
  });
  const client = new ProviderClient();
  const app: FastifyInstance = buildServer({ config, db, cache, client });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  if (address === null || typeof address === 'string') throw new Error('no port');

  return {
    url: `http://127.0.0.1:${address.port}`,
    provider,
    close: async () => {
      await app.close();
      await client.close();
      await provider.close();
    },
  };
}

beforeAll(async () => {
  await waitForRedis(cache);
});

afterEach(async () => {
  await open?.close();
  open = null;
  const keys = [...(await cache.keys(`mg:{t:${TENANT}}:*`)), ...(await cache.keys('mg:cache:*'))];
  if (keys.length > 0) await cache.del(...keys);
  await truncateAll(db);
});

afterAll(async () => {
  await cache.quit();
  await db.end();
});

async function stream(url: string, extra: Record<string, unknown> = {}): Promise<Collected> {
  return readSse(
    await fetch(`${url}/v1/chat/completions`, {
      method: 'POST',
      headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
      body: JSON.stringify({
        model: 'mock-1',
        messages: [{ role: 'user', content: 'hello' }],
        stream: true,
        ...extra,
      }),
    }),
  );
}

async function auditRow(): Promise<Record<string, unknown>> {
  for (let i = 0; i < 80; i += 1) {
    const { rows } = await db.query('SELECT * FROM requests');
    if (rows.length > 0) return rows[0] as Record<string, unknown>;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error('no audit row appeared');
}

describe('why the prefix is re-encoded rather than each delta counted', () => {
  it('over-counts when deltas do not land on token boundaries', () => {
    // Measured, not assumed. A token boundary and a delta boundary are not the
    // same thing, and the encoder cannot merge across a call it never sees.
    const cases: Array<[string, string[]]> = [
      ['a word split across deltas', ['hel', 'lo', ' wor', 'ld']],
      ['japanese characters', ['日', '本', '語', 'を', '話', 'し', 'ま', 'す']],
    ];

    for (const [label, deltas] of cases) {
      const perDelta = deltas.reduce((n, d) => n + countTokens(d), 0);
      const prefix = countTokens(deltas.join(''));
      expect(perDelta, label).toBeGreaterThan(prefix);
    }
  });

  it('agrees exactly when the deltas happen to be token aligned', () => {
    // The half worth stating plainly. Per-delta counting is not always wrong,
    // it is unreliable: with word-aligned English deltas the two agree exactly,
    // so a test written only against that fixture would never catch the bug.
    // Since a gateway cannot choose how a provider chunks its output, the
    // prefix re-encode is the only count that holds in both cases.
    for (const deltas of [
      [' the', ' quick', ' brown', ' fox', ' jumps'],
      [...contentPieces(7, 60)],
    ]) {
      const perDelta = deltas.reduce((n, d) => n + countTokens(d), 0);
      expect(perDelta).toBe(countTokens(deltas.join('')));
    }
  });
});

describe('counting a provider that does not chunk on token boundaries', () => {
  it('records the prefix count, not the sum of the deltas', async () => {
    // The provider here splits every word across two deltas, which is the case
    // that separates a correct counter from a broken one. Counting each delta
    // and adding them up roughly doubles the total; re-encoding the accumulated
    // prefix gives the real number. Usage is suppressed so the estimate is what
    // gets recorded rather than the provider's own figure.
    open = await harness({ fails: 'no-usage', splitDeltas: true });
    const got = await stream(open.url, { max_tokens: 4_000 });
    const row = await auditRow();

    expect(row['token_source']).toBe('estimated');

    const truth = countTokens(got.text);
    const perDelta = got.payloads
      .map((p) => {
        try {
          const c = JSON.parse(p) as { choices?: Array<{ delta?: { content?: unknown } }> };
          const d = c.choices?.[0]?.delta?.content;
          return typeof d === 'string' && d.length > 0 ? countTokens(d) : 0;
        } catch {
          return 0;
        }
      })
      .reduce((a, b) => a + b, 0);

    // The two genuinely disagree on this stream, so the assertion below has
    // something to discriminate.
    expect(perDelta).toBeGreaterThan(truth);
    expect(Number(row['completion_tokens'])).toBe(truth);
  });
});

describe('where the number came from', () => {
  it('prefers the provider usage frame and says so', async () => {
    open = await harness();
    await stream(open.url);
    const row = await auditRow();

    expect(row['token_source']).toBe('provider');
    expect(Number(row['completion_tokens'])).toBeGreaterThan(0);
    expect(Number(row['prompt_tokens'])).toBeGreaterThan(0);
  });

  it('finds usage under x_groq, where there is no top-level field at all', async () => {
    // Code written against OpenAI does `if (chunk.usage)`, which is simply
    // false on every Groq chunk. The failure is a silent zero, never an error.
    open = await harness({ dialect: 'groq' });
    await stream(open.url);
    const row = await auditRow();

    expect(row['token_source']).toBe('provider');
    expect(Number(row['completion_tokens'])).toBeGreaterThan(0);
  });

  it('falls back to an estimate when the usage frame never arrives', async () => {
    open = await harness({ fails: 'no-usage' });
    await stream(open.url);
    const row = await auditRow();

    expect(row['token_source']).toBe('estimated');
    // A missing usage frame does not mean zero tokens were used.
    expect(Number(row['completion_tokens'])).toBeGreaterThan(0);
  });

  it('marks an interrupted stream as a partial estimate', async () => {
    // The case the column exists for. An interrupted stream is exactly when the
    // provider's usage never arrives, and exactly when a number is most needed.
    open = await harness({ fails: 'abort@6' });
    await stream(open.url);
    const row = await auditRow();

    expect(row['token_source']).toBe('partial_estimated');
    expect(row['outcome']).toBe('truncated');
    expect(Number(row['completion_tokens'])).toBeGreaterThan(0);
  });

  it('counts what was actually relayed, not what was reserved', async () => {
    open = await harness();
    const got = await stream(open.url);
    const row = await auditRow();

    // The recorded completion count should match a fresh count of the text the
    // client received. The provider frame wins, so allow a small difference
    // between its framing and ours rather than demanding they be identical.
    const relayed = countTokens(got.text);
    expect(Math.abs(Number(row['completion_tokens']) - relayed)).toBeLessThanOrEqual(2);
    expect(Number(row['est_prompt_tokens'])).toBeGreaterThan(Number(row['prompt_tokens']));
  });
});

describe('stopping a runaway generation', () => {
  it('cuts a stream that runs past the ceiling the request asked for', async () => {
    // The mock ignores max_tokens on purpose, which is what a badly behaved
    // provider does. Without a running count the tenant is billed for a
    // generation nobody bounded.
    open = await harness();
    const got = await stream(open.url, { max_tokens: 5 });

    expect(got.finishReason).toBe('length');
    expect(got.events).toContain('error');
    expect(got.sawDone).toBe(true);

    const full = [...contentPieces(1, 24)].join('');
    expect(got.text.length).toBeLessThan(full.length);
  });

  it('relays exactly what it counted, however the provider packs its frames', async () => {
    // The provider sends the whole response in one packet here. Breaking out of
    // the read loop leaves the rest of that packet buffered in the framer, and
    // flushing it at the end would hand the client the whole answer while the
    // gateway had counted and charged for a fraction of it. The frame-at-a-time
    // case hides this completely.
    open = await harness();
    const got = await stream(open.url, { max_tokens: 5 });
    const row = await auditRow();

    const relayedTokens = countTokens(got.text);
    expect(Math.abs(relayedTokens - Number(row['completion_tokens']))).toBeLessThanOrEqual(2);
  });

  it('names the gateway as the one that stopped it', async () => {
    // A caller debugging a short answer needs to know whose ceiling they hit.
    open = await harness();
    const res = await fetch(`${open.url}/v1/chat/completions`, {
      method: 'POST',
      headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
      body: JSON.stringify({
        model: 'mock-1',
        messages: [{ role: 'user', content: 'hello' }],
        stream: true,
        max_tokens: 5,
      }),
    });
    const body = await res.text();

    expect(body).toContain('modelgate_budget_exceeded');
    expect(body).toContain('token ceiling');
  });

  it('lets a stream inside its ceiling finish normally', async () => {
    open = await harness();
    const got = await stream(open.url, { max_tokens: 500 });

    expect(got.finishReason).toBe('stop');
    expect(got.events).not.toContain('error');
  });
});

describe('the budget after a stream', () => {
  it('settles against the provider total rather than the reservation', async () => {
    open = await harness({ env: { TOKEN_REFILL_PER_SEC: '1' } });
    await stream(open.url);
    const row = await auditRow();

    const spent = 100_000 - Number(await cache.hget(`mg:{t:${TENANT}}:tb:all`, 'tk'));
    const reserved = Number(row['est_prompt_tokens']);
    const recorded = Number(row['prompt_tokens']) + Number(row['completion_tokens']);

    // Charged for what was used, not for the worst case that was held.
    expect(spent).toBeLessThan(reserved);
    expect(Math.abs(spent - recorded)).toBeLessThanOrEqual(2);
  });
});
