/**
 * The OpenAI Chat Completions wire format, reproduced closely enough that a
 * parser written against this mock works against the real thing.
 *
 * Everything the gateway will parse is built here, so there is exactly one
 * place to correct when a provider turns out to differ.
 */

export type Dialect = 'openai' | 'groq';

export interface Usage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
}

export interface FrameOptions {
  id: string;
  created: number;
  model: string;
  dialect: Dialect;
  /**
   * Random padding OpenAI began attaching to streamed chunks. Reproduced
   * because it breaks anything that hashes or compares whole chunk JSON, and a
   * mock without it would let that bug through.
   */
  obfuscation: boolean;
  /** `\r\n\r\n` instead of `\n\n`. Both appear in the wild. */
  crlf: boolean;
}

/** SSE separates events with a blank line. The rest is a `data: ` prefix. */
export function frame(payload: string, opts: Pick<FrameOptions, 'crlf'>): string {
  return opts.crlf ? `data: ${payload}\r\n\r\n` : `data: ${payload}\n\n`;
}

export function doneFrame(opts: Pick<FrameOptions, 'crlf'>): string {
  return frame('[DONE]', opts);
}

function base(opts: FrameOptions): Record<string, unknown> {
  const chunk: Record<string, unknown> = {
    id: opts.id,
    object: 'chat.completion.chunk',
    created: opts.created,
    model: opts.model,
  };
  if (opts.obfuscation) chunk['obfuscation'] = 'x'.repeat(12);
  return chunk;
}

/** First frame of a response: announces the role, carries no text. */
export function roleFrame(opts: FrameOptions): string {
  return frame(
    JSON.stringify({
      ...base(opts),
      choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }],
    }),
    opts,
  );
}

export function contentFrame(opts: FrameOptions, content: string): string {
  return frame(
    JSON.stringify({
      ...base(opts),
      choices: [{ index: 0, delta: { content }, finish_reason: null }],
    }),
    opts,
  );
}

/**
 * The terminal frame. `choices` is NOT empty here, unlike the usage frame that
 * may follow it: a reader that treats "choices is empty" as "the stream ended"
 * gets this one wrong.
 */
export function finishFrame(opts: FrameOptions, reason: string): string {
  return frame(
    JSON.stringify({
      ...base(opts),
      choices: [{ index: 0, delta: {}, finish_reason: reason }],
    }),
    opts,
  );
}

/**
 * Usage placement is the asymmetry that silently zeroes accounting. OpenAI
 * sends a trailing frame with an empty `choices` and a top-level `usage`, and
 * only when `stream_options.include_usage` was requested. Groq has no top-level
 * `usage` on a chunk at all and reports under `x_groq` instead, so code written
 * against OpenAI records nothing and never errors.
 */
export function usageFrame(opts: FrameOptions, usage: Usage): string {
  const payload =
    opts.dialect === 'groq'
      ? { ...base(opts), choices: [], x_groq: { id: `req_${opts.id.slice(9, 21)}`, usage } }
      : { ...base(opts), choices: [], usage };
  return frame(JSON.stringify(payload), opts);
}

/** In-band error, the shape that arrives after a 200 and headers are already sent. */
export function errorDataFrame(opts: FrameOptions, message: string): string {
  return frame(
    JSON.stringify({ error: { message, type: 'server_error', code: null } }),
    opts,
  );
}

/** The other in-band error shape, using a named SSE event rather than bare data. */
export function errorEventFrame(opts: FrameOptions, message: string): string {
  const payload = JSON.stringify({ error: { message, type: 'server_error', code: null } });
  return opts.crlf
    ? `event: error\r\ndata: ${payload}\r\n\r\n`
    : `event: error\ndata: ${payload}\n\n`;
}

/** Groq signals an early stop by hanging an error off the finish chunk. */
export function groqErrorFrame(opts: FrameOptions, message: string): string {
  return frame(
    JSON.stringify({
      ...base(opts),
      choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
      x_groq: { id: `req_${opts.id.slice(9, 21)}`, error: message },
    }),
    opts,
  );
}

/**
 * A frame carrying no choices and no usage. Real streams contain these, and a
 * relay that reaches for `choices[0]` unconditionally throws on the first one.
 */
export function noiseFrame(opts: FrameOptions): string {
  return frame(JSON.stringify({ ...base(opts), choices: [] }), opts);
}

/** The non-streaming response body. */
export function completionBody(
  opts: FrameOptions,
  content: string,
  usage: Usage,
  finishReason = 'stop',
): Record<string, unknown> {
  const body: Record<string, unknown> = {
    id: opts.id,
    object: 'chat.completion',
    created: opts.created,
    model: opts.model,
    choices: [
      { index: 0, message: { role: 'assistant', content }, finish_reason: finishReason },
    ],
    usage,
  };
  // Non-streaming is the one place Groq does document x_groq.
  if (opts.dialect === 'groq') body['x_groq'] = { id: `req_${opts.id.slice(9, 21)}` };
  return body;
}
