import { z } from 'zod';

const KNOWN_PROVIDERS = ['mock-primary', 'mock-backup', 'groq'] as const;
export type ProviderName = (typeof KNOWN_PROVIDERS)[number];

const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  HOST: z.string().min(1).default('0.0.0.0'),
  PORT: z.coerce.number().int().positive().max(65535).default(3100),
  LOG_LEVEL: z
    .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
    .default('info'),
  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().min(1),
  DB_POOL_MAX: z.coerce.number().int().positive().default(20),

  /**
   * Guards every gateway route. There is no default, because a shipped default
   * key is worse than none: it looks like protection and is public knowledge.
   */
  GATEWAY_API_KEY: z.string().min(16),

  /**
   * Tried in order. The first that answers wins, so this is the failover chain
   * rather than a set.
   */
  PROVIDER_ORDER: z
    .string()
    .default('mock-primary,mock-backup')
    .transform((raw) => raw.split(',').map((name) => name.trim()).filter((name) => name !== ''))
    .pipe(z.array(z.enum(KNOWN_PROVIDERS)).min(1)),

  MOCK_PRIMARY_URL: z.string().url().default('http://127.0.0.1:3200'),
  MOCK_BACKUP_URL: z.string().url().default('http://127.0.0.1:3201'),
  GROQ_BASE_URL: z.string().url().default('https://api.groq.com'),
  GROQ_API_KEY: z.string().min(1).optional(),

  /** Per provider, not per request: the chain gets this many tries at each one. */
  MAX_ATTEMPTS_PER_PROVIDER: z.coerce.number().int().positive().max(10).default(2),
  RETRY_BASE_MS: z.coerce.number().int().nonnegative().default(100),
  RETRY_CAP_MS: z.coerce.number().int().positive().default(2_000),

  /**
   * A ceiling on the whole chain. Without it the worst case is providers times
   * attempts times timeout plus backoff, and a caller waits all of that for a
   * 502 that was inevitable after the first provider.
   */
  REQUEST_DEADLINE_MS: z.coerce.number().int().positive().default(60_000),

  /**
   * Exact match only. A prompt that differs by one character is a different
   * question, and deciding when a merely similar prompt is NOT a hit is the
   * interesting half of semantic caching that this does not attempt.
   */
  CACHE_ENABLED: z.enum(['true', 'false']).default('true').transform((v) => v === 'true'),
  CACHE_TTL_SECONDS: z.coerce.number().int().positive().default(300),

  /** Bucket size in tokens. Also the largest single request that can ever run. */
  TOKEN_BUDGET_CAP: z.coerce.number().int().positive().default(100_000),
  TOKEN_REFILL_PER_SEC: z.coerce.number().positive().default(2_000),
  /** How long a reservation is held before the sweep reclaims it. */
  BUDGET_LEASE_TTL_MS: z.coerce.number().int().positive().default(120_000),

  /** How long to wait for a provider to start answering. */
  PROVIDER_HEADERS_TIMEOUT_MS: z.coerce.number().int().positive().default(15_000),
  /**
   * The gap between chunks, not a total deadline. Undici defaults this to five
   * minutes, which is indistinguishable from hanging forever.
   */
  PROVIDER_BODY_TIMEOUT_MS: z.coerce.number().int().positive().default(20_000),
});

export type Config = z.infer<typeof EnvSchema>;

/**
 * Reads and validates the environment. Throws on the first boot rather than
 * failing later on the request that happens to need a missing value.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = EnvSchema.safeParse(env);

  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((issue) => `  ${issue.path.join('.') || '(root)'}: ${issue.message}`)
      .join('\n');
    throw new Error(`Invalid environment:\n${issues}`);
  }

  const config = parsed.data;

  // Groq in the chain without a key would fail every request routed to it with
  // a 401, which the gateway would correctly treat as a reason to fail over.
  // The chain would work and the misconfiguration would stay invisible.
  if (config.PROVIDER_ORDER.includes('groq') && config.GROQ_API_KEY === undefined) {
    throw new Error(
      'Invalid environment:\n  GROQ_API_KEY is required when PROVIDER_ORDER includes groq',
    );
  }

  // A lease reclaimed while its request is still running hands the tokens back
  // to a tenant who is actively spending them, so the same budget gets issued
  // twice. Two body timeouts is the shortest that is comfortably longer than a
  // slow but healthy response.
  if (config.BUDGET_LEASE_TTL_MS < config.PROVIDER_BODY_TIMEOUT_MS * 2) {
    throw new Error(
      `Invalid environment:\n  BUDGET_LEASE_TTL_MS (${config.BUDGET_LEASE_TTL_MS}) must be at least ` +
        `twice PROVIDER_BODY_TIMEOUT_MS (${config.PROVIDER_BODY_TIMEOUT_MS}), or a reservation can be ` +
        'reclaimed while the request holding it is still running',
    );
  }

  if (config.RETRY_CAP_MS < config.RETRY_BASE_MS) {
    throw new Error(
      `Invalid environment:\n  RETRY_CAP_MS (${config.RETRY_CAP_MS}) must be at least ` +
        `RETRY_BASE_MS (${config.RETRY_BASE_MS}), or the cap would clamp the first retry to less than the base`,
    );
  }

  return config;
}
