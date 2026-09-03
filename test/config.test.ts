import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';

const required = {
  DATABASE_URL: 'postgres://modelgate:modelgate@localhost:5433/modelgate',
  REDIS_URL: 'redis://localhost:6380',
  // No entropy on purpose, so a secret scanner never has to judge it.
  GATEWAY_API_KEY: 'x'.repeat(24),
};

describe('loadConfig', () => {
  it('applies defaults when only the required values are present', () => {
    const config = loadConfig(required);

    expect(config.PORT).toBe(3100);
    expect(config.NODE_ENV).toBe('development');
    expect(config.LOG_LEVEL).toBe('info');
    expect(config.DB_POOL_MAX).toBe(20);
  });

  it('names the missing variable when one is absent', () => {
    expect(() => loadConfig({ REDIS_URL: required.REDIS_URL })).toThrow(/DATABASE_URL/);
    expect(() => loadConfig({ DATABASE_URL: required.DATABASE_URL })).toThrow(/REDIS_URL/);
  });

  it('coerces PORT from a string and rejects a non-numeric one', () => {
    expect(loadConfig({ ...required, PORT: '8080' }).PORT).toBe(8080);
    expect(() => loadConfig({ ...required, PORT: 'http' })).toThrow(/PORT/);
  });

  it('rejects a port outside the valid range', () => {
    expect(() => loadConfig({ ...required, PORT: '0' })).toThrow(/PORT/);
    expect(() => loadConfig({ ...required, PORT: '70000' })).toThrow(/PORT/);
  });

  it('rejects a log level outside the supported set', () => {
    expect(() => loadConfig({ ...required, LOG_LEVEL: 'chatty' })).toThrow(/LOG_LEVEL/);
  });

  it('rejects a blank connection string rather than defaulting it', () => {
    // An empty DATABASE_URL is a misconfigured deployment, not a request to use
    // localhost. Defaulting it would connect somewhere nobody intended.
    expect(() => loadConfig({ ...required, DATABASE_URL: '' })).toThrow(/DATABASE_URL/);
  });

  it('requires a gateway key long enough to be worth having', () => {
    // No default is offered anywhere: a shipped default key looks like
    // protection while being public knowledge.
    const { GATEWAY_API_KEY: _omitted, ...withoutKey } = required;
    expect(() => loadConfig(withoutKey)).toThrow(/GATEWAY_API_KEY/);
    expect(() => loadConfig({ ...required, GATEWAY_API_KEY: 'short' })).toThrow(
      /GATEWAY_API_KEY/,
    );
  });
});

describe('provider chain configuration', () => {
  it('splits the order into a list and keeps it ordered', () => {
    const config = loadConfig({ ...required, PROVIDER_ORDER: 'mock-backup, mock-primary' });

    expect(config.PROVIDER_ORDER).toEqual(['mock-backup', 'mock-primary']);
  });

  it('defaults to the two mock providers', () => {
    expect(loadConfig(required).PROVIDER_ORDER).toEqual(['mock-primary', 'mock-backup']);
  });

  it('rejects an unknown provider name', () => {
    expect(() => loadConfig({ ...required, PROVIDER_ORDER: 'mock-primary,openai' })).toThrow(
      /PROVIDER_ORDER/,
    );
  });

  it('rejects an empty order rather than running with no providers', () => {
    expect(() => loadConfig({ ...required, PROVIDER_ORDER: '  ,  ' })).toThrow(
      /PROVIDER_ORDER/,
    );
  });

  it('refuses groq in the chain without a key', () => {
    // Every request routed to a keyless groq would 401, which the gateway
    // correctly treats as a reason to fail over. The chain would keep working
    // and the misconfiguration would never surface.
    expect(() =>
      loadConfig({ ...required, PROVIDER_ORDER: 'groq,mock-primary' }),
    ).toThrow(/GROQ_API_KEY/);

    expect(() =>
      loadConfig({ ...required, PROVIDER_ORDER: 'groq', GROQ_API_KEY: 'gsk-not-real' }),
    ).not.toThrow();
  });

  it('refuses a retry cap below the base delay', () => {
    expect(() =>
      loadConfig({ ...required, RETRY_BASE_MS: '500', RETRY_CAP_MS: '100' }),
    ).toThrow(/RETRY_CAP_MS/);
  });
});
