import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { completionFor } from '../src/mock/tokens.js';
import { chat, startMock, stream, type RunningMock } from './helpers/mock.js';

let mock: RunningMock;

beforeAll(async () => {
  mock = await startMock();
});

afterAll(async () => {
  await mock.close();
});

describe('the happy path', () => {
  it('emits role, content, finish and [DONE] in that order', async () => {
    const got = await stream(mock.url, { seed: 7, tokens: 5 });

    expect(got.transportError).toBeNull();
    expect(got.text).toBe(completionFor(7, 5));
    expect(got.finishReason).toBe('stop');
    expect(got.sawDone).toBe(true);
    expect(got.unparsable).toEqual([]);
  });

  it('sets the headers that keep a stream unbuffered', async () => {
    const res = await chat(mock.url, { tokens: 2 });

    expect(res.headers.get('content-type')).toBe('text/event-stream; charset=utf-8');
    // no-transform is what stops a compression layer collapsing the stream into
    // one burst, which no assertion on the frames themselves would catch.
    expect(res.headers.get('cache-control')).toBe('no-cache, no-transform');
    expect(res.headers.get('x-accel-buffering')).toBe('no');
    // A length would contradict a stream whose size is not known up front.
    expect(res.headers.get('content-length')).toBeNull();

    await res.body?.cancel();
  });

  it('holds one id, model and object across every frame', async () => {
    const got = await stream(mock.url, { seed: 3, tokens: 6, includeUsage: true });
    const chunks = got.payloads.map((p) => JSON.parse(p) as Record<string, unknown>);

    expect(chunks.length).toBeGreaterThan(6);
    expect(new Set(chunks.map((c) => c['id'])).size).toBe(1);
    expect(new Set(chunks.map((c) => c['model'])).size).toBe(1);
    expect(new Set(chunks.map((c) => c['object']))).toEqual(
      new Set(['chat.completion.chunk']),
    );
  });

  it('repeats byte for byte under one seed and differs under another', async () => {
    // Phase 5 sweeps a kill offset across the same response, which only means
    // anything if the response is identical every run.
    const a = await stream(mock.url, { seed: 42, tokens: 12 });
    const b = await stream(mock.url, { seed: 42, tokens: 12 });
    const c = await stream(mock.url, { seed: 43, tokens: 12 });

    expect(a.text).toBe(b.text);
    expect(a.text).not.toBe(c.text);
  });

  it('carries multi-byte characters through intact', async () => {
    // A relay that decodes each TCP chunk on its own splits these, and an
    // English-only fixture would never show it.
    const got = await stream(mock.url, { seed: 5, tokens: 60 });

    expect(got.text).toBe(completionFor(5, 60));
    expect(got.text).not.toContain('�');
  });

  it('separates frames with CRLF when asked', async () => {
    const got = await stream(mock.url, { seed: 7, tokens: 5, crlf: true });

    expect(got.text).toBe(completionFor(7, 5));
    expect(got.sawDone).toBe(true);
  });
});

describe('usage reporting', () => {
  it('sends usage only when it was requested', async () => {
    const without = await stream(mock.url, { tokens: 4 });
    const with_ = await stream(mock.url, { tokens: 4, includeUsage: true });

    expect(without.usage).toBeNull();
    expect(with_.usage).toMatchObject({ completion_tokens: 4 });
  });

  it('puts usage under x_groq in the groq dialect, not at the top level', async () => {
    // Code written against OpenAI does `if (chunk.usage)`, which is simply false
    // on every Groq chunk. The result is a silent zero, not an error.
    const got = await stream(mock.url, { tokens: 4, includeUsage: true, dialect: 'groq' });
    const frames = got.payloads.map((p) => JSON.parse(p) as Record<string, unknown>);

    expect(frames.some((f) => f['usage'] !== undefined)).toBe(false);
    expect(got.usage).toMatchObject({ completion_tokens: 4 });
  });

  it('can end without usage even though it was requested', async () => {
    const got = await stream(mock.url, { tokens: 4, includeUsage: true, fail: 'no-usage' });

    expect(got.usage).toBeNull();
    expect(got.sawDone).toBe(true);
  });
});

describe('failures that break the connection', () => {
  it('abort leaves no finish reason and no [DONE]', async () => {
    const got = await stream(mock.url, { tokens: 20, fail: 'abort@5' });

    expect(got.transportError).not.toBeNull();
    expect(got.finishReason).toBeNull();
    expect(got.sawDone).toBe(false);
    expect(got.text.length).toBeGreaterThan(0);
  });

  it('fin also ends without a terminal frame', async () => {
    const got = await stream(mock.url, { tokens: 20, fail: 'fin@5' });

    expect(got.finishReason).toBeNull();
    expect(got.sawDone).toBe(false);
  });

  it('clean-end truncates with no error at all', async () => {
    // The dangerous one. A correct HTTP end, no socket error, no timeout: the
    // only evidence is a terminal frame that never arrived.
    const got = await stream(mock.url, { tokens: 20, fail: 'clean-end@5' });

    expect(got.transportError).toBeNull();
    expect(got.finishReason).toBeNull();
    expect(got.sawDone).toBe(false);
    expect(got.text.length).toBeGreaterThan(0);
  });

  it('truncate-json leaves an unparsable frame behind', async () => {
    const got = await stream(mock.url, { tokens: 20, fail: 'truncate-json@5' });

    expect(got.unparsable.length).toBeGreaterThan(0);
    expect(got.sawDone).toBe(false);
  });
});

describe('failures carried in the stream itself', () => {
  it('error-data delivers an error object after a 200', async () => {
    const res = await chat(mock.url, { tokens: 20, fail: 'error-data@4' });
    expect(res.status).toBe(200);

    const got = await (await import('./helpers/mock.js')).readSse(res);
    const errored = got.payloads
      .map((p) => {
        try {
          return JSON.parse(p) as { error?: { message?: string } };
        } catch {
          return {};
        }
      })
      .filter((c) => c.error !== undefined);

    expect(errored.length).toBe(1);
    expect(got.sawDone).toBe(false);
  });

  it('error-event uses a named event rather than bare data', async () => {
    const got = await stream(mock.url, { tokens: 20, fail: 'error-event@4' });

    expect(got.events).toContain('error');
    expect(got.sawDone).toBe(false);
  });

  it('x-groq-error stops early but still closes the stream properly', async () => {
    const got = await stream(mock.url, { tokens: 20, fail: 'x-groq-error@4' });

    const withError = got.payloads
      .map((p) => JSON.parse(p) as { x_groq?: { error?: string } })
      .filter((c) => c.x_groq?.error !== undefined);

    expect(withError.length).toBe(1);
    expect(got.sawDone).toBe(true);
  });
});

describe('failures that look perfectly healthy', () => {
  it('content_filter completes the protocol while truncating the answer', async () => {
    // Protocol-perfect and semantically truncated. Treating a clean [DONE] as
    // success records a partial answer as a complete one.
    const got = await stream(mock.url, { tokens: 20, fail: 'filter-stop@6' });

    expect(got.transportError).toBeNull();
    expect(got.sawDone).toBe(true);
    expect(got.finishReason).toBe('content_filter');
  });

  it('length does the same at the token ceiling', async () => {
    const got = await stream(mock.url, { tokens: 20, fail: 'length-stop@6' });

    expect(got.sawDone).toBe(true);
    expect(got.finishReason).toBe('length');
  });
});

describe('failures that require a timer to notice', () => {
  it('hang sends headers and some frames, then nothing', async () => {
    // No error and no end. Only an inter-chunk idle timer catches this, and it
    // is a different timer from the one guarding the headers.
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 400);
    try {
      const got = await stream(mock.url, {
        tokens: 40,
        fail: 'hang@3',
        signal: controller.signal,
      });

      expect(got.text.length).toBeGreaterThan(0);
      expect(got.sawDone).toBe(false);
      expect(got.transportError?.name).toBe('AbortError');
    } finally {
      clearTimeout(timer);
    }
  });

  it('slow spaces the frames out', async () => {
    const started = Date.now();
    const got = await stream(mock.url, { tokens: 6, fail: 'slow@30' });

    expect(got.sawDone).toBe(true);
    // Five gaps of 30ms. Asserting a floor, not a ceiling: a ceiling would be
    // flaky on a loaded runner and would not prove anything extra.
    expect(Date.now() - started).toBeGreaterThanOrEqual(120);
  });
});

describe('shapes that break naive parsing', () => {
  it('noise interleaves frames carrying no choices', async () => {
    const got = await stream(mock.url, { tokens: 12, fail: 'noise' });
    const empty = got.payloads
      .map((p) => JSON.parse(p) as { choices?: unknown[] })
      .filter((c) => Array.isArray(c.choices) && c.choices.length === 0);

    expect(empty.length).toBeGreaterThan(0);
    expect(got.sawDone).toBe(true);
  });

  it('tool-split cuts arguments mid-escape and mid-character', async () => {
    const got = await stream(mock.url, { tokens: 4, fail: 'tool-split' });
    const args = got.payloads
      .map((p) => JSON.parse(p) as { choices?: Array<{ delta?: { tool_calls?: Array<{ function?: { arguments?: string } }> } }> })
      .flatMap((c) => c.choices?.[0]?.delta?.tool_calls ?? [])
      .map((t) => t.function?.arguments ?? '')
      .join('');

    expect(args.length).toBeGreaterThan(0);
    // Only the joined string is valid JSON. Any per-delta parse throws.
    expect(() => JSON.parse(args) as unknown).not.toThrow();
    expect((JSON.parse(args) as { note: string }).note).toBe('café 日本');
  });
});

describe('rate limiting', () => {
  it('returns 429 with groq-shaped headers', async () => {
    const res = await chat(mock.url, { fail: '429' });

    expect(res.status).toBe(429);
    const reset = res.headers.get('x-ratelimit-reset-requests');
    // A Go duration, not a number of seconds. parseInt gives 2, which is the
    // whole reason this is worth reproducing.
    expect(reset).toBe('2m59.56s');
    expect(Number.parseInt(reset ?? '', 10)).toBe(2);
    expect(res.headers.get('x-ratelimit-limit-requests')).toBe('14400');

    await res.json();
  });
});

describe('non-streaming', () => {
  it('answers with a single completion body', async () => {
    const res = await fetch(`${mock.url}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-mock-seed': '9', 'x-mock-tokens': '8' },
      body: JSON.stringify({ model: 'mock-1', messages: [{ role: 'user', content: 'hi' }] }),
    });
    const body = (await res.json()) as {
      object: string;
      choices: Array<{ message: { content: string }; finish_reason: string }>;
      usage: { completion_tokens: number };
    };

    expect(res.status).toBe(200);
    expect(body.object).toBe('chat.completion');
    expect(body.choices[0]?.message.content).toBe(completionFor(9, 8));
    expect(body.choices[0]?.finish_reason).toBe('stop');
    expect(body.usage.completion_tokens).toBe(8);
  });
});

describe('request rejection', () => {
  it('rejects an unknown failure mode instead of streaming healthily', async () => {
    // A typo that silently disabled the fault would leave a failover test
    // asserting against a perfectly good stream, and it would pass.
    const res = await chat(mock.url, { fail: 'abrot@5' });

    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { message: string } };
    expect(body.error.message).toMatch(/unknown x-mock-fail mode/);
  });

  it('rejects a request with no messages', async () => {
    const res = await fetch(`${mock.url}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'mock-1', messages: [] }),
    });

    expect(res.status).toBe(400);
    await res.json();
  });

  it('rejects structured output with streaming in the groq dialect', async () => {
    // A 400 every provider would also return, so the gateway must treat it as
    // fatal rather than failing over and burning the next provider on it too.
    const res = await chat(mock.url, {
      dialect: 'groq',
      body: { response_format: { type: 'json_object' } },
    });

    expect(res.status).toBe(400);
    await res.json();
  });
});
