/**
 * Incremental SSE framing.
 *
 * The gateway has to forward bytes it also needs to understand, which is the
 * awkward part of a streaming proxy. The compromise here is to buffer at most
 * one incomplete frame, tens of bytes, and hand on whole frames. That is not
 * "buffering the stream", and it is what lets token counting later work on an
 * object rather than on a byte offset.
 */

export interface SseFrame {
  /**
   * The original bytes, terminator included, so relaying is byte faithful. A
   * frame is never re-serialised from its parsed form: providers add fields
   * that would be dropped, and a re-encode changes the bytes for no reason.
   */
  raw: string;
  /** Payload of every `data:` line in this frame, in order. */
  data: string[];
  /** Name from an `event:` line. Chat Completions is data-only, so usually null. */
  event: string | null;
}

const SEPARATOR = /\r\n\r\n|\n\n|\r\r/;

export class SseFramer {
  /**
   * One decoder for the life of the connection.
   *
   * A multi-byte character can be split across TCP chunks, and a decoder
   * created per chunk turns the halves into replacement characters. It works
   * perfectly against English fixtures and mangles everything else.
   */
  readonly #decoder = new TextDecoder('utf-8');
  #buffer = '';

  /** Yields every complete frame the chunk finished. */
  *push(chunk: Uint8Array): Generator<SseFrame> {
    // stream:true is what makes the decoder hold a partial character back
    // instead of emitting a replacement for it.
    this.#buffer += this.#decoder.decode(chunk, { stream: true });
    yield* this.#drain();
  }

  /** Feeds already-decoded text. Used by tests and by replay. */
  *pushText(text: string): Generator<SseFrame> {
    this.#buffer += text;
    yield* this.#drain();
  }

  *#drain(): Generator<SseFrame> {
    for (;;) {
      const match = SEPARATOR.exec(this.#buffer);
      if (match?.index === undefined) return;

      const body = this.#buffer.slice(0, match.index);
      const raw = this.#buffer.slice(0, match.index + match[0].length);
      this.#buffer = this.#buffer.slice(match.index + match[0].length);

      // A separator with nothing before it is a keepalive or padding. Forward
      // the bytes, but there is no event in it to report.
      if (body.trim() === '') continue;

      yield parseFrame(body, raw);
    }
  }

  /**
   * Whatever is left when the connection ends.
   *
   * A frame without its blank line is exactly what a mid-stream kill leaves
   * behind, so it is still worth reporting: it is often the last thing the
   * provider managed to say.
   */
  flush(): SseFrame | null {
    const body = this.#buffer;
    this.#buffer = '';
    if (body.trim() === '') return null;
    return parseFrame(body, body);
  }

  /** True when a partial frame is being held. */
  get pending(): boolean {
    return this.#buffer.length > 0;
  }
}

function parseFrame(body: string, raw: string): SseFrame {
  const data: string[] = [];
  let event: string | null = null;

  for (const line of body.split(/\r\n|\n|\r/)) {
    if (line.startsWith(':')) continue; // comment, used as a keepalive
    const colon = line.indexOf(':');
    const field = colon === -1 ? line : line.slice(0, colon);
    // One optional space after the colon is part of the framing, not the value.
    const value = colon === -1 ? '' : line.slice(colon + 1).replace(/^ /, '');

    if (field === 'data') data.push(value);
    else if (field === 'event') event = value;
  }

  return { raw, data, event };
}

export const DONE_SENTINEL = '[DONE]';

export function isDone(frame: SseFrame): boolean {
  return frame.data.some((d) => d.trim() === DONE_SENTINEL);
}

/** Parsed chunk fields the gateway cares about. Everything else is passed through. */
export interface ChunkView {
  finishReason: string | null;
  /** True when the frame carried a content delta, which is what commits a stream. */
  hasContent: boolean;
  error: { message?: string } | null;
  /**
   * Held constant across a response. Captured so a terminal frame the gateway
   * has to invent still belongs to the same stream the client was reading.
   */
  id: string | null;
  model: string | null;
  created: number | null;
}

export function viewChunk(frame: SseFrame): ChunkView | null {
  if (isDone(frame)) return null;

  for (const payload of frame.data) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
    } catch {
      // A torn frame. The caller decides what that means; here it is simply not
      // a chunk anyone can read fields out of.
      continue;
    }

    const chunk = parsed as {
      id?: unknown;
      model?: unknown;
      created?: unknown;
      choices?: Array<{ delta?: { content?: unknown }; finish_reason?: string | null }>;
      error?: { message?: string };
      x_groq?: { error?: string };
    };

    const choice = chunk.choices?.[0];
    const content = choice?.delta?.content;

    return {
      finishReason: choice?.finish_reason ?? null,
      hasContent: typeof content === 'string' && content.length > 0,
      error:
        chunk.error !== undefined
          ? chunk.error
          : chunk.x_groq?.error !== undefined
            ? { message: chunk.x_groq.error }
            : null,
      id: typeof chunk.id === 'string' ? chunk.id : null,
      model: typeof chunk.model === 'string' ? chunk.model : null,
      created: typeof chunk.created === 'number' ? chunk.created : null,
    };
  }

  return null;
}
