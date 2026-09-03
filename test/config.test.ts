import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';

const required = {
  DATABASE_URL: 'postgres://modelgate:modelgate@localhost:5433/modelgate',
  REDIS_URL: 'redis://localhost:6380',
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
});
