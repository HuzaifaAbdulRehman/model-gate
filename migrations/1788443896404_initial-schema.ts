import type { MigrationBuilder } from 'node-pg-migrate';

export const shorthands: undefined = undefined;

export async function up(pgm: MigrationBuilder): Promise<void> {
  pgm.sql(`
    CREATE TYPE redaction_class AS ENUM (
      'api_key', 'private_key', 'jwt', 'url_credentials',
      'email', 'phone', 'credit_card', 'national_id', 'ip_address',
      'high_entropy'
    );
  `);

  // Metadata and payloads are split because they have different lifetimes: the
  // numbers here are what the benchmarks query for months, while the prompt
  // text is only useful while someone is still debugging that request. One
  // table would force the longer retention on both.
  //
  // Partitioned from the first migration on purpose. Postgres requires the
  // partition key in every primary key and unique constraint, so retrofitting
  // partitioning later means rewriting every key -- much cheaper to accept the
  // composite keys now than to discover the requirement mid-build.
  pgm.sql(`
    CREATE TABLE requests (
      id                 uuid NOT NULL DEFAULT gen_random_uuid(),
      created_at         timestamptz NOT NULL DEFAULT now(),
      tenant_id          text NOT NULL,
      idempotency_key    text,
      model              text NOT NULL,
      stream             boolean NOT NULL,
      outcome            text NOT NULL,
      http_status        smallint,
      final_provider     text,
      attempt_count      smallint NOT NULL DEFAULT 0,
      cache_result       text NOT NULL DEFAULT 'miss',
      est_prompt_tokens  integer,
      prompt_tokens      integer,
      completion_tokens  integer,
      token_source       text NOT NULL,
      ttfb_ms            integer,
      total_ms           integer,
      redaction_classes  redaction_class[] NOT NULL DEFAULT '{}',
      redaction_count    integer NOT NULL DEFAULT 0,
      entropy_suspect    boolean NOT NULL DEFAULT false,
      PRIMARY KEY (id, created_at),
      CONSTRAINT requests_outcome_check CHECK (
        outcome IN ('ok', 'failed', 'truncated', 'continued', 'client_abort')
      ),
      -- 'estimated' is the expected value whenever a stream is interrupted, not
      -- a rare fallback: a provider that never sent its usage frame did not
      -- report zero tokens, it reported nothing. Recording which source the
      -- number came from is what keeps the two apart.
      CONSTRAINT requests_token_source_check CHECK (
        token_source IN ('provider', 'estimated', 'partial_estimated')
      ),
      CONSTRAINT requests_cache_result_check CHECK (
        cache_result IN ('hit', 'miss', 'bypass')
      ),
      CONSTRAINT requests_tenant_not_blank CHECK (length(btrim(tenant_id)) > 0),
      CONSTRAINT requests_attempt_count_non_negative CHECK (attempt_count >= 0),
      CONSTRAINT requests_token_counts_non_negative CHECK (
        coalesce(est_prompt_tokens, 0) >= 0
        AND coalesce(prompt_tokens, 0) >= 0
        AND coalesce(completion_tokens, 0) >= 0
      ),
      CONSTRAINT requests_redaction_count_non_negative CHECK (redaction_count >= 0)
    ) PARTITION BY RANGE (created_at);
  `);

  // No foreign key to requests. Payloads are dropped on a shorter clock than
  // metadata, and a reference would tie the two retention schedules together
  // for a guarantee this gateway does not need: a payload row whose metadata
  // has aged out is not a bug, it is the retention policy working.
  pgm.sql(`
    CREATE TABLE request_payloads (
      request_id         uuid NOT NULL,
      created_at         timestamptz NOT NULL,
      prompt             text NOT NULL,
      completion         text,
      -- HMAC of the RAW prompt. Redaction runs before this table is written, so
      -- without a fingerprint taken upstream there is no way to tell whether two
      -- audit rows came from the same original prompt.
      prompt_fingerprint bytea NOT NULL,
      redactions         jsonb NOT NULL DEFAULT '[]',
      -- Bumped whenever a detection pattern changes, so a gap in what was caught
      -- can be explained by which engine wrote the row rather than guessed at.
      engine_version     text NOT NULL,
      PRIMARY KEY (request_id, created_at),
      CONSTRAINT request_payloads_engine_version_not_blank
        CHECK (length(btrim(engine_version)) > 0),
      CONSTRAINT request_payloads_redactions_is_array
        CHECK (jsonb_typeof(redactions) = 'array')
    ) PARTITION BY RANGE (created_at);
  `);

  // One row per provider call. This is where the failover story is evidenced:
  // tokens_flushed and committed together say whether the client had already
  // seen output when the provider died, which is the whole question mid-stream
  // failover has to answer.
  pgm.sql(`
    CREATE TABLE request_attempts (
      request_id          uuid NOT NULL,
      created_at          timestamptz NOT NULL,
      attempt_no          smallint NOT NULL,
      provider            text NOT NULL,
      outcome             text NOT NULL,
      http_status         smallint,
      error_code          text,
      bytes_flushed       integer,
      tokens_flushed      integer,
      committed           boolean NOT NULL,
      continuation_mode   text,
      seam_overlap_chars  integer,
      -- Nullable by necessity: providers only carry their request id on the
      -- first or final chunk, so an attempt that died in between never had one.
      provider_request_id text,
      queue_time_ms       integer,
      prompt_time_ms      integer,
      completion_time_ms  integer,
      latency_ms          integer,
      PRIMARY KEY (request_id, created_at, attempt_no),
      CONSTRAINT request_attempts_outcome_check CHECK (
        outcome IN ('ok', 'http_error', 'timeout', 'stream_abort',
                    'rate_limited', 'truncated_clean')
      ),
      CONSTRAINT request_attempts_continuation_mode_check CHECK (
        continuation_mode IS NULL
        OR continuation_mode IN ('none', 'user_turn', 'prefill')
      ),
      CONSTRAINT request_attempts_attempt_no_positive CHECK (attempt_no > 0),
      CONSTRAINT request_attempts_provider_not_blank
        CHECK (length(btrim(provider)) > 0),
      -- A continuation only means anything once bytes were already committed.
      -- Recording one on an uncommitted attempt would misreport a clean
      -- failover as a seam the client actually saw.
      CONSTRAINT request_attempts_continuation_requires_commit CHECK (
        continuation_mode IS NULL
        OR continuation_mode = 'none'
        OR committed
      )
    ) PARTITION BY RANGE (created_at);
  `);

  // A DEFAULT partition on every partitioned table. Without one, an insert whose
  // timestamp falls outside every defined range fails outright -- so a clock
  // skew of a few minutes past a month boundary would take the gateway down
  // rather than land a row somewhere slightly untidy.
  //
  // Named partitions cover the current month and the two after it so ordinary
  // traffic never reaches DEFAULT, because rows sitting in DEFAULT block the
  // later creation of a partition covering their range.
  pgm.sql(`
    DO $$
    DECLARE
      target  text;
      months  int;
      start_m date;
      end_m   date;
    BEGIN
      FOREACH target IN ARRAY ARRAY['requests', 'request_payloads', 'request_attempts']
      LOOP
        EXECUTE format(
          'CREATE TABLE %I PARTITION OF %I DEFAULT', target || '_default', target
        );

        FOR months IN 0..2 LOOP
          start_m := date_trunc('month', now()) + make_interval(months => months);
          end_m   := start_m + interval '1 month';
          EXECUTE format(
            'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
            target || '_' || to_char(start_m, 'YYYYMM'), target, start_m, end_m
          );
        END LOOP;
      END LOOP;
    END $$;
  `);

  pgm.sql(`
    CREATE INDEX requests_tenant_recent ON requests (tenant_id, created_at DESC);
  `);

  // Failures are the minority on a healthy gateway, so a partial index stays
  // small and successful requests cost nothing to maintain in it.
  pgm.sql(`
    CREATE INDEX requests_recent_problems ON requests (created_at DESC)
      WHERE outcome <> 'ok';
  `);

  // Idempotency is enforced here rather than by a unique index on requests, and
  // this table is deliberately NOT partitioned.
  //
  // Postgres requires every partition key column inside a unique constraint on
  // a partitioned table. Adding created_at to satisfy that makes the constraint
  // useless for this purpose: every request has a distinct created_at, so
  // (tenant_id, idempotency_key, created_at) is unique even for two genuinely
  // duplicate requests. The index would look like a guard and enforce nothing.
  //
  // An unpartitioned table is the only place Postgres will enforce the pair
  // globally. It stays small because keys expire on a much shorter clock than
  // the audit rows, and it is swept by created_at rather than by dropping
  // partitions.
  //
  // The key is scoped to the tenant, because an idempotency key is only unique
  // to whoever chose it.
  pgm.sql(`
    CREATE TABLE idempotency_keys (
      tenant_id          text NOT NULL,
      idempotency_key    text NOT NULL,
      request_id         uuid NOT NULL,
      -- Needed to reach the audit row: requests is partitioned on created_at,
      -- so the id alone does not locate it without scanning every partition.
      request_created_at timestamptz NOT NULL,
      created_at         timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (tenant_id, idempotency_key),
      CONSTRAINT idempotency_keys_tenant_not_blank
        CHECK (length(btrim(tenant_id)) > 0),
      CONSTRAINT idempotency_keys_key_not_blank
        CHECK (length(btrim(idempotency_key)) > 0)
    );
  `);

  pgm.sql(`
    CREATE INDEX idempotency_keys_expiry ON idempotency_keys (created_at);
  `);

  // Non-unique: uniqueness lives in idempotency_keys. This only exists so an
  // audit row can be found by the key a caller remembers sending.
  pgm.sql(`
    CREATE INDEX requests_idempotency_lookup
      ON requests (tenant_id, idempotency_key)
      WHERE idempotency_key IS NOT NULL;
  `);

  pgm.sql(`
    CREATE INDEX requests_redaction_classes ON requests USING GIN (redaction_classes);
  `);

  pgm.sql(`
    CREATE INDEX request_payloads_fingerprint ON request_payloads (prompt_fingerprint);
  `);

  pgm.sql(`
    CREATE INDEX request_attempts_provider_recent
      ON request_attempts (provider, created_at DESC);
  `);
}

export async function down(pgm: MigrationBuilder): Promise<void> {
  pgm.sql('DROP TABLE IF EXISTS idempotency_keys;');
  // Dropping the parent takes every partition and index with it.
  pgm.sql('DROP TABLE IF EXISTS request_attempts;');
  pgm.sql('DROP TABLE IF EXISTS request_payloads;');
  pgm.sql('DROP TABLE IF EXISTS requests;');
  pgm.sql('DROP TYPE IF EXISTS redaction_class;');
}
