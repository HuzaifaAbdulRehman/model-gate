import type { FastifyInstance } from 'fastify';
import { buildMockServer, type MockOptions } from '../../src/mock/server.js';

export interface RunningMock {
  app: FastifyInstance;
  url: string;
  close: () => Promise<void>;
}

/**
 * A real listening server on an ephemeral port, not `app.inject`. Half the
 * failure modes are socket-level, and an injected request has no socket to
 * destroy.
 */
export async function startMock(options: MockOptions = {}): Promise<RunningMock> {
  const app = buildMockServer(options);
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('mock server did not bind a TCP port');
  }
  return {
    app,
    url: `http://127.0.0.1:${address.port}`,
    close: () => app.close(),
  };
}

export interface Collected {
  /** Every `data:` payload, in order, excluding the [DONE] sentinel. */
  payloads: string[];
  /** Names from any `event:` lines. Chat Completions is data-only, so normally empty. */
  events: string[];
  /** Concatenated delta content, which is what a client would have displayed. */
  text: string;
  sawDone: boolean;
  finishReason: string | null;
  usage: Record<string, number> | null;
  /** Set when the connection failed rather than ending. */
  transportError: Error | null;
  /** A payload that was not valid JSON, which is what a torn frame looks like. */
  unparsable: string[];
}

interface ChunkShape {
  choices?: Array<{ delta?: { content?: string }; finish_reason?: string | null }>;
  usage?: Record<string, number>;
  x_groq?: { usage?: Record<string, number>; error?: string };
  error?: { message?: string };
}

/**
 * Reads an SSE response into something assertable. Frames are split on the
 * blank-line separator rather than on a fixed byte count, because a chunk
 * boundary lands mid-frame often enough that any other approach tears one.
 */
export async function readSse(response: Response): Promise<Collected> {
  const out: Collected = {
    payloads: [],
    events: [],
    text: '',
    sawDone: false,
    finishReason: null,
    usage: null,
    transportError: null,
    unparsable: [],
  };

  if (response.body === null) return out;

  const decoder = new TextDecoder('utf-8');
  const reader = response.body.getReader();
  let buffer = '';

  const consume = (frame: string): void => {
    for (const line of frame.split(/\r\n|\n/)) {
      if (line.startsWith('event:')) {
        out.events.push(line.slice(6).trim());
        continue;
      }
      if (!line.startsWith('data:')) continue;

      const payload = line.slice(5).trim();
      if (payload === '[DONE]') {
        out.sawDone = true;
        continue;
      }
      out.payloads.push(payload);

      let chunk: ChunkShape;
      try {
        chunk = JSON.parse(payload) as ChunkShape;
      } catch {
        out.unparsable.push(payload);
        continue;
      }

      const choice = chunk.choices?.[0];
      if (choice?.delta?.content !== undefined) out.text += choice.delta.content;
      if (choice?.finish_reason != null) out.finishReason = choice.finish_reason;
      // Never read usage from the final frame alone: providers differ on where
      // they put it, and at least one sends it second-to-last.
      if (chunk.usage !== undefined) out.usage = chunk.usage;
      if (chunk.x_groq?.usage !== undefined) out.usage = chunk.x_groq.usage;
    }
  };

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      let split = buffer.search(/\r\n\r\n|\n\n/);
      while (split !== -1) {
        const match = /\r\n\r\n|\n\n/.exec(buffer.slice(split));
        const width = match?.[0].length ?? 2;
        consume(buffer.slice(0, split));
        buffer = buffer.slice(split + width);
        split = buffer.search(/\r\n\r\n|\n\n/);
      }
    }
  } catch (err) {
    out.transportError = err as Error;
  }

  // A frame left without its blank line is exactly what a mid-frame kill leaves
  // behind, so it still counts as seen.
  if (buffer.trim() !== '') consume(buffer);

  return out;
}

export interface StreamArgs {
  fail?: string;
  seed?: number;
  tokens?: number;
  dialect?: 'openai' | 'groq';
  includeUsage?: boolean;
  crlf?: boolean;
  signal?: AbortSignal;
  body?: Record<string, unknown>;
}

export function chatHeaders(args: StreamArgs): Record<string, string> {
  const headers: Record<string, string> = { 'content-type': 'application/json' };
  if (args.fail !== undefined) headers['x-mock-fail'] = args.fail;
  if (args.seed !== undefined) headers['x-mock-seed'] = String(args.seed);
  if (args.tokens !== undefined) headers['x-mock-tokens'] = String(args.tokens);
  if (args.dialect !== undefined) headers['x-mock-dialect'] = args.dialect;
  if (args.crlf === true) headers['x-mock-crlf'] = '1';
  return headers;
}

export async function chat(url: string, args: StreamArgs = {}): Promise<Response> {
  const body = {
    model: 'mock-1',
    messages: [{ role: 'user', content: 'hello there' }],
    stream: true,
    ...(args.includeUsage === true ? { stream_options: { include_usage: true } } : {}),
    ...args.body,
  };
  return fetch(`${url}/v1/chat/completions`, {
    method: 'POST',
    headers: chatHeaders(args),
    body: JSON.stringify(body),
    ...(args.signal !== undefined ? { signal: args.signal } : {}),
  });
}

/** Opens a stream and reads it to completion or failure. */
export async function stream(url: string, args: StreamArgs = {}): Promise<Collected> {
  return readSse(await chat(url, args));
}
