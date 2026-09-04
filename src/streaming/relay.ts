import { once } from 'node:events';
import type { Writable } from 'node:stream';
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
        if (view?.hasContent === true) contentFrames += 1;

        if (out.sink === null) {
          pending.push(frame.raw);
          // Content is what proves the provider is really answering, rather
          // than having merely accepted the connection.
          if (view?.hasContent === true) await doCommit();
        } else {
          await write(frame.raw);
        }
      }
    }

    // Whatever the provider managed to say before the socket went is still
    // worth forwarding, and it is often the most informative frame.
    const tail = framer.flush();
    if (tail !== null) {
      const view = viewChunk(tail);
      if (view?.finishReason != null) finishReason = view.finishReason;
      if (out.sink === null) pending.push(tail.raw);
      else await write(tail.raw);
    }
  } catch (err) {
    if ((err as Error).name === 'AbortError') clientGone = true;
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
    if (outcome === 'stream_abort' || outcome === 'in_band_error') {
      const message = errorMessage ?? 'the provider stopped responding mid-stream';
      await write(
        `event: error\ndata: ${JSON.stringify({
          error: { message, type: 'upstream_error', code: 'modelgate_interrupted' },
        })}\n\n`,
      );
      await write(
        `data: ${JSON.stringify({
          id: identity.id ?? 'chatcmpl-modelgate',
          object: 'chat.completion.chunk',
          created: identity.created ?? Math.floor(Date.now() / 1000),
          model: identity.model ?? 'unknown',
          choices: [{ index: 0, delta: {}, finish_reason: 'modelgate_interrupted' }],
        })}\n\n`,
      );
    }

    await write(`data: ${DONE_SENTINEL}\n\n`);
    committedSink.end();
  }

  return {
    outcome,
    committed: committedSink !== null,
    bytesFlushed,
    contentFrames,
    finishReason,
    errorMessage,
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
