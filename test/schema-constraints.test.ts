import type pg from 'pg';
import { afterAll, afterEach, describe, expect, it } from 'vitest';
import { createTestPool, truncateAll } from './helpers/db.js';

const db: pg.Pool = createTestPool();

afterEach(async () => {
  await truncateAll(db);
});

afterAll(async () => {
  await db.end();
});

async function insertRequest(overrides: Record<string, unknown> = {}) {
  const row = {
    tenant_id: 'acme',
    model: 'mock-1',
    stream: false,
    outcome: 'ok',
    token_source: 'provider',
    ...overrides,
  };
  const columns = Object.keys(row);
  const placeholders = columns.map((_, i) => `$${i + 1}`);
  return db.query(
    // redaction_classes is cast on the way out because node-postgres ships no
    // parser for a custom enum's array OID and would hand back the raw Postgres
    // literal '{}' as a string. The cast keeps the enum's integrity in the
    // schema without needing a runtime type parser registered at boot.
    `INSERT INTO requests (${columns.join(', ')}) VALUES (${placeholders.join(', ')})
     RETURNING id, created_at, cache_result, attempt_count,
               redaction_classes::text[] AS redaction_classes`,
    Object.values(row),
  );
}

describe('requests partitioning', () => {
  it('routes an ordinary row into the month partition, not the default', async () => {
    const { rows } = await insertRequest();

    const { rows: located } = await db.query(
      `SELECT tableoid::regclass::text AS partition FROM requests WHERE id = $1`,
      [rows[0].id],
    );

    expect(located[0].partition).toMatch(/^requests_\d{6}$/);
  });

  it('has named partitions covering at least the next 30 days', async () => {
    // A canary for partition maintenance, which does not exist yet. The
    // migration creates only the current month and the two after it, so a
    // long-lived database eventually routes live traffic into DEFAULT -- and
    // once rows are sitting in DEFAULT, Postgres refuses to create a partition
    // covering their range, so the repair is a data move rather than a DDL.
    // Failing here is the warning that maintenance is now overdue.
    const thirtyDaysOut = new Date(Date.now() + 30 * 86_400_000).toISOString();
    const { rows } = await insertRequest({ created_at: thirtyDaysOut });

    const { rows: located } = await db.query(
      `SELECT tableoid::regclass::text AS partition FROM requests WHERE id = $1`,
      [rows[0].id],
    );

    expect(located[0].partition).not.toBe('requests_default');
  });

  it('accepts a timestamp outside every named range instead of failing', async () => {
    // A clock skew past a month boundary must not take the gateway down. The
    // DEFAULT partition is what turns that into an untidy row rather than a 500.
    const { rows } = await insertRequest({ created_at: '2100-01-01T00:00:00Z' });

    const { rows: located } = await db.query(
      `SELECT tableoid::regclass::text AS partition FROM requests WHERE id = $1`,
      [rows[0].id],
    );

    expect(located[0].partition).toBe('requests_default');
  });
});

describe('requests constraints', () => {
  it('applies the documented defaults', async () => {
    const { rows } = await insertRequest();

    expect(rows[0].cache_result).toBe('miss');
    expect(rows[0].attempt_count).toBe(0);
    expect(rows[0].redaction_classes).toEqual([]);
  });

  it('rejects an outcome outside the known set', async () => {
    await expect(insertRequest({ outcome: 'sort-of-worked' })).rejects.toThrow(
      /requests_outcome_check/,
    );
  });

  it('rejects a token_source outside the known set', async () => {
    // A missing usage frame means the count is estimated, not that it is
    // unknown-and-therefore-null. Every row has to say where its number came
    // from, or the accounting cannot be audited.
    await expect(insertRequest({ token_source: 'guessed' })).rejects.toThrow(
      /requests_token_source_check/,
    );
  });

  it('rejects a blank tenant', async () => {
    await expect(insertRequest({ tenant_id: '   ' })).rejects.toThrow(
      /requests_tenant_not_blank/,
    );
  });

  it('rejects a negative token count', async () => {
    await expect(insertRequest({ completion_tokens: -1 })).rejects.toThrow(
      /requests_token_counts_non_negative/,
    );
  });

  it('rejects an unknown redaction class', async () => {
    await expect(insertRequest({ redaction_classes: ['{passport}'] })).rejects.toThrow();
  });
});

describe('idempotency uniqueness', () => {
  async function claim(tenant: string, key: string) {
    const { rows } = await insertRequest({ tenant_id: tenant, idempotency_key: key });
    return db.query(
      `INSERT INTO idempotency_keys (tenant_id, idempotency_key, request_id, request_created_at)
       VALUES ($1, $2, $3, $4)`,
      [tenant, key, rows[0].id, rows[0].created_at],
    );
  }

  it('rejects a repeated key within one tenant', async () => {
    await claim('acme', 'k1');

    await expect(claim('acme', 'k1')).rejects.toThrow(/idempotency_keys_pkey/);
  });

  it('allows the same key for a different tenant', async () => {
    // A key is only unique to whoever chose it, so scoping it globally would let
    // one tenant's key block another's request.
    await claim('acme', 'k1');

    await expect(claim('globex', 'k1')).resolves.toBeDefined();
  });

  it('does not attempt to enforce the key on requests itself', async () => {
    // Guards against someone re-adding a unique index to requests. It cannot
    // work: Postgres requires the partition key inside any unique constraint on
    // a partitioned table, and created_at is distinct for every row, so the
    // index would accept every duplicate while looking like a guard.
    await insertRequest({ idempotency_key: 'k1' });

    await expect(insertRequest({ idempotency_key: 'k1' })).resolves.toBeDefined();
  });

  it('allows many rows with no key at all', async () => {
    // NULL means "no key supplied", which is not a claim that two such requests
    // are the same request.
    await insertRequest();
    await insertRequest();

    const { rows } = await db.query('SELECT count(*)::int AS n FROM requests');
    expect(rows[0].n).toBe(2);
  });

  it('rejects a blank key rather than storing it', async () => {
    await expect(claim('acme', '   ')).rejects.toThrow(/idempotency_keys_key_not_blank/);
  });
});

describe('request_attempts constraints', () => {
  async function insertAttempt(overrides: Record<string, unknown> = {}) {
    const { rows } = await insertRequest();
    const row = {
      request_id: rows[0].id,
      created_at: rows[0].created_at,
      attempt_no: 1,
      provider: 'mock',
      outcome: 'ok',
      committed: false,
      ...overrides,
    };
    const columns = Object.keys(row);
    const placeholders = columns.map((_, i) => `$${i + 1}`);
    return db.query(
      `INSERT INTO request_attempts (${columns.join(', ')})
       VALUES (${placeholders.join(', ')}) RETURNING attempt_no`,
      Object.values(row),
    );
  }

  it('accepts a plain uncommitted attempt', async () => {
    await expect(insertAttempt()).resolves.toBeDefined();
  });

  it('rejects a continuation on an attempt that never committed bytes', async () => {
    // Recording a continuation on an uncommitted attempt would misreport a clean
    // pre-commit failover as a seam the client actually saw.
    await expect(
      insertAttempt({ committed: false, continuation_mode: 'user_turn' }),
    ).rejects.toThrow(/request_attempts_continuation_requires_commit/);
  });

  it('accepts a continuation once bytes were committed', async () => {
    await expect(
      insertAttempt({ committed: true, continuation_mode: 'user_turn' }),
    ).resolves.toBeDefined();
  });

  it("accepts continuation_mode 'none' regardless of commit", async () => {
    await expect(
      insertAttempt({ committed: false, continuation_mode: 'none' }),
    ).resolves.toBeDefined();
  });

  it('rejects attempt_no below one', async () => {
    await expect(insertAttempt({ attempt_no: 0 })).rejects.toThrow(
      /request_attempts_attempt_no_positive/,
    );
  });

  it('rejects a duplicate attempt number for one request', async () => {
    const { rows } = await insertRequest();
    const base = {
      request_id: rows[0].id,
      created_at: rows[0].created_at,
      provider: 'mock',
      outcome: 'ok',
      committed: false,
    };
    const insert = (attempt_no: number) =>
      db.query(
        `INSERT INTO request_attempts (request_id, created_at, attempt_no, provider, outcome, committed)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [base.request_id, base.created_at, attempt_no, base.provider, base.outcome, base.committed],
      );

    await insert(1);
    // The partition's own key, not the parent's: primary keys on a partitioned
    // table are enforced by an index per partition.
    await expect(insert(1)).rejects.toThrow(/request_attempts_\d{6}_pkey/);
  });
});

describe('request_payloads constraints', () => {
  it('rejects a redactions value that is not a JSON array', async () => {
    const { rows } = await insertRequest();

    await expect(
      db.query(
        `INSERT INTO request_payloads
           (request_id, created_at, prompt, prompt_fingerprint, redactions, engine_version)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [rows[0].id, rows[0].created_at, 'hello', Buffer.from('fp'), '{}', 'v1'],
      ),
    ).rejects.toThrow(/request_payloads_redactions_is_array/);
  });

  it('rejects a blank engine version', async () => {
    // A gap in what was caught has to be explainable by which engine wrote the
    // row, so an unlabelled row is worse than no row.
    const { rows } = await insertRequest();

    await expect(
      db.query(
        `INSERT INTO request_payloads
           (request_id, created_at, prompt, prompt_fingerprint, engine_version)
         VALUES ($1, $2, $3, $4, $5)`,
        [rows[0].id, rows[0].created_at, 'hello', Buffer.from('fp'), '  '],
      ),
    ).rejects.toThrow(/request_payloads_engine_version_not_blank/);
  });
});
