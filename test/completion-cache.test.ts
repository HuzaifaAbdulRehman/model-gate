import { describe, expect, it } from 'vitest';
import { CompletionCache } from '../src/cache/completions.js';
import type { Cache } from '../src/redis.js';
import type { ChatRequest } from '../src/providers/profile.js';

// The key derivation is pure, so nothing needs to be running to test it.
const cache = new CompletionCache({} as Cache, { enabled: true, ttlSeconds: 60 });

const base: ChatRequest = {
  model: 'mock-1',
  messages: [{ role: 'user', content: 'hello' }],
};

describe('cache key derivation', () => {
  it('is stable for the same request', () => {
    expect(cache.key(base, 'acme')).toBe(cache.key({ ...base }, 'acme'));
  });

  it('separates tenants', () => {
    // Sharing an entry across tenants would serve one caller's prompt back
    // through another caller's completion.
    expect(cache.key(base, 'acme')).not.toBe(cache.key(base, 'globex'));
  });

  it('ignores the order fields were written in', () => {
    const reordered: ChatRequest = {
      messages: [{ role: 'user', content: 'hello' }],
      model: 'mock-1',
    };

    expect(cache.key(reordered, 'acme')).toBe(cache.key(base, 'acme'));
  });

  it('ignores field order inside a message too', () => {
    const nested: ChatRequest = {
      model: 'mock-1',
      messages: [{ content: 'hello', role: 'user' }],
    };

    expect(cache.key(nested, 'acme')).toBe(cache.key(base, 'acme'));
  });

  it('respects the order of the messages themselves', () => {
    // Field order carries no meaning; conversation order carries all of it.
    const swapped: ChatRequest = {
      model: 'mock-1',
      messages: [
        { role: 'assistant', content: 'hello' },
        { role: 'user', content: 'hi' },
      ],
    };
    const other: ChatRequest = {
      model: 'mock-1',
      messages: [
        { role: 'user', content: 'hi' },
        { role: 'assistant', content: 'hello' },
      ],
    };

    expect(cache.key(swapped, 'acme')).not.toBe(cache.key(other, 'acme'));
  });

  it('separates requests that differ only in a sampling parameter', () => {
    expect(cache.key({ ...base, temperature: 0.7 }, 'acme')).not.toBe(
      cache.key({ ...base, temperature: 0.2 }, 'acme'),
    );
  });

  it('separates models', () => {
    expect(cache.key({ ...base, model: 'other' }, 'acme')).not.toBe(cache.key(base, 'acme'));
  });

  it('carries a version, so a shared redis outliving a deploy cannot confuse shapes', () => {
    expect(cache.key(base, 'acme')).toMatch(/^mg:cache:v\d+:acme:[0-9a-f]{64}$/);
  });
});
