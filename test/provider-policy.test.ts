import { describe, expect, it } from 'vitest';
import { backoffDelay, retryDelay } from '../src/gateway/backoff.js';
import { classifyError, classifyStatus } from '../src/providers/errors.js';
import {
  groqProfile,
  mockProfile,
  parseGoDuration,
  shapeRequest,
  type ChatRequest,
} from '../src/providers/profile.js';

// A profile with no status override. Written as an empty object rather than an
// explicit undefined, which exactOptionalPropertyTypes rejects for good reason:
// "absent" and "present but undefined" are not the same claim.
const plain = {};
const groq = groqProfile();

describe('status classification', () => {
  it('treats a malformed request as fatal', () => {
    // Every provider rejects the same bad body, so failing over would multiply
    // one clean error into a call against each provider in the chain.
    for (const status of [400, 404, 405, 413, 414, 415, 422, 501]) {
      expect(classifyStatus(status, plain)).toBe('fatal');
    }
  });

  it('fails over rather than retrying a credential problem', () => {
    // Our key for this provider is wrong. That says nothing about the next
    // provider, which has its own.
    expect(classifyStatus(401, plain)).toBe('failover');
    expect(classifyStatus(403, plain)).toBe('failover');
  });

  it('fails over on a rate limit instead of waiting out the window', () => {
    expect(classifyStatus(429, plain)).toBe('failover');
  });

  it('retries a server error on the same provider first', () => {
    for (const status of [500, 502, 503, 504]) {
      expect(classifyStatus(status, plain)).toBe('retry');
    }
  });

  it('retries the transient 4xx codes', () => {
    for (const status of [408, 409, 425]) {
      expect(classifyStatus(status, plain)).toBe('retry');
    }
  });

  it('defaults an unrecognised 4xx to fatal', () => {
    // Being wrong this way returns one honest error. Being wrong the other way
    // repeats it against every provider.
    expect(classifyStatus(418, plain)).toBe('fatal');
    expect(classifyStatus(451, plain)).toBe('fatal');
  });

  it('lets a profile override the shared rules', () => {
    // Groq documents 498 as retryable and 499 as never retryable, which is the
    // opposite of what the numbers suggest.
    expect(classifyStatus(498, groq)).toBe('retry');
    expect(classifyStatus(499, groq)).toBe('fatal');
    // And the override does not leak into providers that did not ask for it.
    expect(classifyStatus(498, plain)).toBe('fatal');
  });
});

describe('transport error classification', () => {
  it('retries a dropped or refused connection', () => {
    for (const code of ['ECONNRESET', 'ECONNREFUSED', 'UND_ERR_SOCKET', 'UND_ERR_BODY_TIMEOUT']) {
      expect(classifyError(Object.assign(new Error('boom'), { code }))).toBe('retry');
    }
  });

  it('does not retry a caller-initiated abort', () => {
    // The client cancelled. Retrying would spend a provider call on a response
    // nobody is waiting for.
    const aborted = Object.assign(new Error('aborted'), { name: 'AbortError' });
    expect(classifyError(aborted)).toBe('fatal');
  });
});

describe('go duration parsing', () => {
  it('reads the compound form providers actually send', () => {
    // parseInt('2m59.56s') is 2, which would be read as two seconds instead of
    // nearly three minutes and send a retry straight back into a closed window.
    expect(parseGoDuration('2m59.56s')).toBe(179_560);
    expect(Number.parseInt('2m59.56s', 10)).toBe(2);
  });

  it('reads the simple forms', () => {
    expect(parseGoDuration('7.66s')).toBe(7_660);
    expect(parseGoDuration('500ms')).toBe(500);
    expect(parseGoDuration('1h30m')).toBe(5_400_000);
  });

  it('treats a bare number as seconds', () => {
    expect(parseGoDuration('180')).toBe(180_000);
  });

  it('returns null rather than guessing', () => {
    expect(parseGoDuration(undefined)).toBeNull();
    expect(parseGoDuration('')).toBeNull();
    expect(parseGoDuration('soon')).toBeNull();
  });
});

describe('backoff', () => {
  const options = { baseMs: 100, capMs: 2_000 };

  it('grows exponentially in its ceiling', () => {
    // The draw is uniform below the ceiling, so the ceiling is what the test
    // pins. A fixed random makes that observable.
    expect(backoffDelay(0, { ...options, random: () => 0.999 })).toBe(99);
    expect(backoffDelay(1, { ...options, random: () => 0.999 })).toBe(199);
    expect(backoffDelay(2, { ...options, random: () => 0.999 })).toBe(399);
  });

  it('caps the ceiling', () => {
    expect(backoffDelay(20, { ...options, random: () => 0.999 })).toBe(1_998);
  });

  it('can return zero, which is what makes it jitter', () => {
    // Without a floor of zero the delays cluster, and a fleet that failed
    // together retries together.
    expect(backoffDelay(5, { ...options, random: () => 0 })).toBe(0);
  });

  it('prefers the provider reset over a local guess', () => {
    expect(retryDelay(0, 750, options)).toBe(750);
  });

  it('caps a reset that is further away than we will wait', () => {
    // Three minutes is longer than the caller will hold on for. Moving to
    // another provider beats sleeping.
    expect(retryDelay(0, 179_560, options)).toBe(2_000);
  });

  it('falls back to backoff when the provider said nothing', () => {
    expect(retryDelay(0, null, { ...options, random: () => 0.5 })).toBe(50);
  });
});

describe('request shaping', () => {
  const request: ChatRequest = {
    model: 'llama',
    messages: [{ role: 'user', content: 'hi' }],
    temperature: 0,
    logprobs: true,
    n: 4,
  };

  it('drops parameters the provider does not support', () => {
    const shaped = shapeRequest(groq, request);

    expect('reject' in shaped).toBe(false);
    if ('reject' in shaped) return;
    expect(shaped.body['logprobs']).toBeUndefined();
    expect(shaped.body['n']).toBe(1);
  });

  it('clamps temperature itself so the audit log is honest', () => {
    // Left to the provider, the request would record 0 while the model ran at
    // something else.
    const shaped = shapeRequest(groq, request);
    if ('reject' in shaped) throw new Error('unexpected rejection');

    expect(shaped.body.temperature).toBe(1e-8);
  });

  it('leaves the caller request untouched', () => {
    shapeRequest(groq, request);

    expect(request['logprobs']).toBe(true);
    expect(request['n']).toBe(4);
    expect(request.temperature).toBe(0);
  });

  it('rejects a combination the provider will refuse, before dispatch', () => {
    const shaped = shapeRequest(groq, {
      ...request,
      stream: true,
      response_format: { type: 'json_object' },
    });

    expect('reject' in shaped).toBe(true);
  });

  it('changes nothing for a provider with no quirks', () => {
    const shaped = shapeRequest(mockProfile('mock', 'http://127.0.0.1:1'), request);
    if ('reject' in shaped) throw new Error('unexpected rejection');

    expect(shaped.body).toEqual(request);
  });
});
