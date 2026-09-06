import { once } from 'node:events';
import type { Writable } from 'node:stream';
import type { Usage } from '../providers/profile.js';
import { DONE_SENTINEL, SseFramer, isDone, viewChunk } from './framer.js';

/**
 * How a stream ended, and whether the client can tell.
 *
 * The distinction that matters is between an ending the protocol vouched for
 * and one it did not. A socket that closes after `[DONE]` is finished. A socket
 * that closes having never sent a finish reason is a failure, even though
 * nothing threw and no timeout fired.
 */
export type StreamOutcome =
  /** Terminal frame seen. The answer is whole. */
  | 'complete'
  /**
   * Protocol-perfect and semantically truncated: a clean `[DONE]` after the
   * model hit a ceiling or a filter. Treating this as success records a partial
   * answer as a complete one.
   */
  | 'truncated_clean'
  /** The provider said, in the stream, that it had failed. */
  | 'in_band_error'
  /** Ended with no terminal evidence at all. No error, no timeout, just gone. */
  | 'stream_abort'
  /** The caller hung up. Not a provider failure. */
  | 'client_gone';

export interface RelayResult {
  outcome: StreamOutcome;
  /**
   * Whether any bytes reached the client. Before this is true a clean HTTP
   * error is still possible; after it, the status is already sent and every
   * error has to travel in-band.
   */
  committed: boolean;
  bytesFlushed: number;
  contentFrames: number;
  finishReason: string | null;
  errorMessage: string | null;
  /** The relayed completion, kept for the audit log. */
  completionText: string;
  /** Provider response id observed on any frame, when one was sent. */
  providerRequestId: string | null;
  /** True when the gateway's whole-request ceiling ended the provider body. */
  deadlineExceeded: boolean;
  /**
   * Best available completion count, and where it came from.
   *
   * A provider that sent no usage frame did not report zero, it reported
   * nothing. Interrupted and cancelled streams are exactly the cases where it
   * never arrives, so `estimated` is the ordinary value here rather than an
   * edge case, and the difference has to survive into the audit row.
   */
  completionTokens: number;
  providerUsage: Usage | null;
  tokenSource: 'provider' | 'estimated';
  /** True when the gateway stopped the stream for running past its ceiling. */
  budgetStopped: boolean;
}

export interface RelayOptions {
  body: AsyncIterable<Uint8Array>;
  /**
   * Called once, when the stream is judged live. Returns the sink to write
   * into. Nothing is written before this, which is what keeps a clean HTTP
   * error available for as long as possible.
   */
  commit: () => Writable;
  /**
   * Commit anyway after this long, even with no content yet.
   *
   * Without it a slow model would sit behind the gateway with the client
   * seeing nothing, and time-to-first-token is the number a streaming API is
   * judged on.
   */
  commitDeadlineMs: number;
  signal: AbortSignal;
  /** Separate from the caller signal so a gateway timeout is not logged as a disconnect. */
  deadlineSignal?: AbortSignal;
  /**
   * Counts a whole string. Called on the accumulated completion, never on one
   * delta.
   *
   * Counting deltas separately and adding them up over-counts badly, because a
   * token boundary and a delta boundary are not the same thing: the encoder
   * cannot merge across a call it never sees. Re-encoding the prefix costs a
   * couple of milliseconds on a long response and is exact.
   */
  countTokens?: (text: string) => number;
  /** Where this provider puts usage. Never assume the top level. */
  extractUsage?: (chunk: unknown) => Usage | null;
  /**
   * Stop the stream if the completion passes this.
   *
   * The reason the cheap running count exists at all: without it a model that
   * ignores max_tokens bills a tenant for a generation nobody bounded.
   */
  maxCompletionTokens?: number;
  /** Recount cadence. Whichever comes first. */
  recountEveryDeltas?: number;
  recountEveryMs?: number;
}

const TRUNCATING_REASONS = new Set(['length', 'content_filter']);

export async function relayStream(options: RelayOptions): Promise<RelayResult> {
  const framer = new SseFramer();

  // Held in an object rather than a bare local. It is assigned inside a
  // closure, and the compiler would otherwise still believe it is null after
  // the call that set it.
  const out: { sink: Writable | null } = { sink: null };
  /** Frames seen before the commit point, replayed in order once it passes. */
  const pending: string[] = [];

  let bytesFlushed = 0;
  let contentFrames = 0;
  let finishReason: string | null = null;
  let errorMessage: string | null = null;
  let sawDone = false;
  let clientGone = false;
  let transportFailed = false;

  const count = options.countTokens ?? ((text: string) => text.length);
  const recountEveryDeltas = options.recountEveryDeltas ?? 64;
  const recountEveryMs = options.recountEveryMs ?? 250;
  // max_tokens is interpreted by the provider's tokenizer, which can differ
  // slightly from ours. A small margin prevents a valid final token from being
  // mislabeled as runaway while keeping a provider that ignores the limit
  // bounded close to the requested amount.
  const runawayTokenLimit =
    options.maxCompletionTokens === undefined
      ? undefined
      : options.maxCompletionTokens + Math.max(4, Math.ceil(options.maxCompletionTokens * 0.1));
  /** Kept as pieces and joined on demand, so the common path does no copying. */
  const completionChunks: string[] = [];
  let completionTokens = 0;
  let completionBytes = 0;
  let providerUsage: Usage | null = null;
  let budgetStopped = false;
  let deltasSinceRecount = 0;
  let lastRecountAt = Date.now();
  let nextTripwireByte =
    runawayTokenLimit === undefined ? Number.POSITIVE_INFINITY : runawayTokenLimit + 1;
  let stopping = false;

  const recount = (): void => {
    completionTokens = count(completionChunks.join(''));
    deltasSinceRecount = 0;
    lastRecountAt = Date.now();
    if (runawayTokenLimit !== undefined) {
      // Between exact samples, each appended byte can add at most one token.
      // Move the cheap trigger forward by the remaining allowance so an early
      // byte/token mismatch does not turn the rest of the stream into a
      // per-delta re-encode.
      const remaining = Math.max(0, runawayTokenLimit - completionTokens);
      nextTripwireByte = completionBytes + Math.max(1, remaining);
    }
  };
  const identity: { id: string | null; model: string | null; created: number | null } = {
    id: null,
    model: null,
    created: null,
  };

  const write = async (raw: string): Promise<void> => {
    const sink = out.sink;
    if (sink === null || clientGone) return;
    bytesFlushed += Buffer.byteLength(raw);
    if (!sink.write(raw)) {
      // The signal is not optional. `drain` never fires on a destroyed stream,
      // so an unsignalled wait deadlocks the moment a client hangs up. Ignoring
      // the return value instead is worse: a slow client then buffers the whole
      // response in the gateway's memory.
      await once(sink, 'drain', { signal: options.signal });
    }
  };

  const doCommit = async (): Promise<void> => {
    if (out.sink !== null || clientGone) return;
    out.sink = options.commit();
    for (const raw of pending) await write(raw);
    pending.length = 0;
  };

  // Fires even if not one frame arrives, which is the case the deadline exists
  // for. Cleared in the finally, or it keeps the process alive after the
  // request is long gone.
  let deadlineTimer: NodeJS.Timeout | undefined = setTimeout(() => {
    void doCommit().catch(() => undefined);
  }, options.commitDeadlineMs);

  const onAbort = (): void => {
    clientGone = true;
  };
  options.signal.addEventListener('abort', onAbort, { once: true });

  try {
    for await (const chunk of options.body) {
      if (options.signal.aborted) {
        clientGone = true;
        break;
      }
      if (stopping) break;

      for (const frame of framer.push(chunk)) {
        if (isDone(frame)) {
          // Swallowed on purpose. Exactly one sentinel is emitted, by us, at
          // the end. Forwarding the provider's as well would send two after a
          // failover in phase five.
          sawDone = true;
          continue;
        }

        const view = viewChunk(frame);
        if (view?.id != null) identity.id = view.id;
        if (view?.model != null) identity.model = view.model;
        if (view?.created != null) identity.created = view.created;
        if (view?.error != null) {
          errorMessage = view.error.message ?? 'provider reported an error mid-stream';
        }
        if (view?.finishReason != null) finishReason = view.finishReason;

        // Read on every frame, never only the last one. Providers disagree
        // about where the usage chunk sits, and at least one puts it
        // second to last, so a last-frame-only reader silently records zero.
        const usage = options.extractUsage?.(view?.parsed);
        if (usage != null) providerUsage = usage;

        if (view?.hasContent === true && view.content !== null) {
          contentFrames += 1;
          completionChunks.push(view.content);
          completionBytes += Buffer.byteLength(view.content);
          deltasSinceRecount += 1;

          // Counter two. Exact, because it re-encodes the whole prefix, and
          // affordable because it runs on a cadence rather than per delta.
          if (
            deltasSinceRecount >= recountEveryDeltas ||
            Date.now() - lastRecountAt >= recountEveryMs
          ) {
            recount();
          }

          // Counter one: UTF-8 bytes, updated on every delta. A token consumes
          // at least one byte, so crossing the safety threshold in bytes is the
          // cheap signal to run the exact counter. Counting deltas here misses
          // a provider that packs a large response into only a few frames.
          if (
            runawayTokenLimit !== undefined &&
            completionBytes >= nextTripwireByte
          ) {
            recount();
            if (completionTokens > runawayTokenLimit) {
              budgetStopped = true;
              stopping = true;
            }
          }
        }

        if (out.sink === null) {
          pending.push(frame.raw);
          // Content is what proves the provider is really answering, rather
          // than having merely accepted the connection.
          if (view?.hasContent === true) await doCommit();
        } else {
          await write(frame.raw);
        }

        if (stopping) break;
      }

      // Check before asking the async iterator for another chunk. Waiting for
      // the next loop header pulls one more packet from the provider after the
      // gateway has already decided to stop.
      if (stopping) break;
    }

    // Whatever the provider managed to say before the socket went is still
    // worth forwarding, and it is often the most informative frame.
    //
    // Only when the stream ended on its own, though. Breaking out of the read
    // loop leaves every frame the current chunk still held sitting in the
    // framer, and flushing that would hand the client the whole rest of a
    // response the gateway had just decided to stop, uncounted. It shows up
    // only when a provider's frames arrive in one packet, which is why the
    // frame-at-a-time case looks perfectly healthy.
    const tail = stopping || clientGone ? null : framer.flush();
    if (tail !== null) {
      const view = viewChunk(tail);
      if (view?.finishReason != null) finishReason = view.finishReason;
      if (out.sink === null) pending.push(tail.raw);
      else await write(tail.raw);
    }
  } catch {
    // AbortError is also what Undici raises when the gateway deadline cancels
    // the upstream body. The caller signal is the source of truth here; using
    // the error name records gateway timeouts as client disconnects.
    if (options.signal.aborted) clientGone = true;
    else transportFailed = true;
  } finally {
    clearTimeout(deadlineTimer);
    deadlineTimer = undefined;
    options.signal.removeEventListener('abort', onAbort);
  }

  const outcome = decideOutcome({
    clientGone,
    transportFailed,
    errorMessage,
    finishReason,
    sawDone,
  });
  const deadlineExceeded =
    options.deadlineSignal?.aborted === true && transportFailed && !clientGone;

  // A stream that ended properly still has to reach the client, even if it
  // never produced a content delta: an empty answer is a valid answer.
  if (out.sink === null && !clientGone && (outcome === 'complete' || outcome === 'truncated_clean')) {
    await doCommit();
  }

  const committedSink = out.sink;
  if (committedSink !== null && !clientGone) {
    // The status line went out as 200 the moment the first byte was written, and
    // an HTTP status is final. So a stream that dies after committing has to say
    // so inside itself. Ending quietly instead would hand the caller a truncated
    // answer that looks complete, which is the failure this whole distinction
    // exists to prevent.
    if (outcome === 'stream_abort' || outcome === 'in_band_error' || budgetStopped) {
      const message = budgetStopped
        ? 'stopped by the gateway: the completion passed the token ceiling safety margin'
        : deadlineExceeded
          ? 'stopped by the gateway: the request passed its deadline'
          : (errorMessage ?? 'the provider stopped responding mid-stream');
      await write(
        `event: error\ndata: ${JSON.stringify({
          error: {
            message,
            type: budgetStopped
              ? 'rate_limit_exceeded'
              : deadlineExceeded
                ? 'timeout_error'
                : 'upstream_error',
            code: budgetStopped
              ? 'modelgate_budget_exceeded'
              : deadlineExceeded
                ? 'gateway_deadline_exceeded'
                : 'modelgate_interrupted',
          },
        })}\n\n`,
      );
      await write(
        `data: ${JSON.stringify({
          id: identity.id ?? 'chatcmpl-modelgate',
          object: 'chat.completion.chunk',
          created: identity.created ?? Math.floor(Date.now() / 1000),
          model: identity.model ?? 'unknown',
          choices: [
            {
              index: 0,
              delta: {},
              // `length` because that is what a caller's client already knows
              // how to read: the answer stopped at a ceiling. The error frame
              // above says whose ceiling it was.
              finish_reason: budgetStopped ? 'length' : 'modelgate_interrupted',
            },
          ],
        })}\n\n`,
      );
    }

    await write(`data: ${DONE_SENTINEL}\n\n`);
    committedSink.end();
  }

  // Counter three, and the last word when it exists. Before it, one final exact
  // pass so the recorded number is never a stale sample from the previous
  // cadence tick.
  recount();

  return {
    outcome,
    committed: committedSink !== null,
    bytesFlushed,
    contentFrames,
    finishReason,
    errorMessage,
    completionText: completionChunks.join(''),
    providerRequestId: identity.id,
    deadlineExceeded,
    completionTokens: providerUsage?.completion_tokens ?? completionTokens,
    providerUsage,
    tokenSource: providerUsage !== null ? 'provider' : 'estimated',
    budgetStopped,
  };
}

function decideOutcome(state: {
  clientGone: boolean;
  transportFailed: boolean;
  errorMessage: string | null;
  finishReason: string | null;
  sawDone: boolean;
}): StreamOutcome {
  if (state.clientGone) return 'client_gone';
  if (state.errorMessage !== null) return 'in_band_error';
  if (state.finishReason !== null && TRUNCATING_REASONS.has(state.finishReason)) {
    return 'truncated_clean';
  }
  // A finish reason without the sentinel is benign: only the terminator was
  // lost, and the answer itself is whole.
  if (state.finishReason !== null || (state.sawDone && !state.transportFailed)) {
    return 'complete';
  }
  return 'stream_abort';
}
