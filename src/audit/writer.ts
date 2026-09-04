import type { Db } from '../db.js';
import type { AttemptRecord } from '../gateway/dispatch.js';
import type { Redacted, RedactionClass, RedactionEntry } from './redact.js';

export interface AuditRecord {
  requestId: string;
  tenantId: string;
  idempotencyKey: string | null;
  model: string;
  stream: boolean;
  outcome: 'ok' | 'failed' | 'truncated' | 'continued' | 'client_abort';
  httpStatus: number | null;
  finalProvider: string | null;
  cacheResult: 'hit' | 'miss' | 'bypass';
  estPromptTokens: number | null;
  promptTokens: number | null;
  completionTokens: number | null;
  tokenSource: 'provider' | 'estimated' | 'partial_estimated';
  ttfbMs: number | null;
  totalMs: number | null;
  redactionClasses: RedactionClass[];
  entropySuspect: boolean;
}

/**
 * Only accepts text that has been through the redactor.
 *
 * This is the whole enforcement mechanism. `Redacted` cannot be constructed
 * anywhere else, so handing a raw prompt to the audit log stops being something
 * a reviewer has to spot and becomes something the compiler refuses.
 */
export interface AuditPayload {
  prompt: Redacted;
  completion: Redacted | null;
  /**
   * HMAC of the RAW prompt. Redaction has already happened by the time a row is
   * written, so without a fingerprint taken upstream there is no way to tell
   * whether two audit rows came from the same original text.
   */
  promptFingerprint: Buffer;
  redactions: RedactionEntry[];
  engineVersion: string;
}

export class AuditWriter {
  readonly #db: Db;

  constructor(db: Db) {
    this.#db = db;
  }

  async write(
    record: AuditRecord,
    payload: AuditPayload,
    attempts: readonly AttemptRecord[],
  ): Promise<void> {
    const client = await this.#db.connect();
    try {
      await client.query('BEGIN');

      // created_at is taken once and reused for the child rows, because it is
      // the partition key: letting each table call now() separately would put
      // the pieces of one request in different partitions on a month boundary.
      const inserted = await client.query<{ created_at: Date }>(
        `INSERT INTO requests (
           id, tenant_id, idempotency_key, model, stream, outcome, http_status,
           final_provider, attempt_count, cache_result, est_prompt_tokens,
           prompt_tokens, completion_tokens, token_source, ttfb_ms, total_ms,
           redaction_classes, redaction_count, entropy_suspect
         ) VALUES (
           $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
           $17::text[]::redaction_class[], $18, $19
         )
         RETURNING created_at`,
        [
          record.requestId,
          record.tenantId,
          record.idempotencyKey,
          record.model,
          record.stream,
          record.outcome,
          record.httpStatus,
          record.finalProvider,
          attempts.length,
          record.cacheResult,
          record.estPromptTokens,
          record.promptTokens,
          record.completionTokens,
          record.tokenSource,
          record.ttfbMs,
          record.totalMs,
          record.redactionClasses,
          payload.redactions.length,
          record.entropySuspect,
        ],
      );

      const createdAt = inserted.rows[0]?.created_at;
      if (createdAt === undefined) throw new Error('audit insert returned no row');

      await client.query(
        `INSERT INTO request_payloads (
           request_id, created_at, prompt, completion, prompt_fingerprint,
           redactions, engine_version
         ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`,
        [
          record.requestId,
          createdAt,
          payload.prompt,
          payload.completion,
          payload.promptFingerprint,
          JSON.stringify(payload.redactions),
          payload.engineVersion,
        ],
      );

      for (const attempt of attempts) {
        await client.query(
          `INSERT INTO request_attempts (
             request_id, created_at, attempt_no, provider, outcome, http_status,
             error_code, committed, provider_request_id, latency_ms
           ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
          [
            record.requestId,
            createdAt,
            attempt.attemptNo,
            attempt.provider,
            attempt.outcome,
            attempt.httpStatus,
            attempt.errorCode,
            attempt.committed,
            attempt.providerRequestId,
            attempt.latencyMs,
          ],
        );
      }

      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }
}
