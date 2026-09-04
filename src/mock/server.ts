import type { ServerResponse } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import Fastify, { type FastifyInstance } from 'fastify';
import { z } from 'zod';
import { parseFailure, type Failure } from './failures.js';
import {
  completionBody,
  contentFrame,
  doneFrame,
  errorDataFrame,
  errorEventFrame,
  finishFrame,
  groqErrorFrame,
  noiseFrame,
  roleFrame,
  usageFrame,
  type Dialect,
  type FrameOptions,
  type Usage,
} from './sse.js';
import { completionId, contentPieces } from './tokens.js';

const DEFAULT_TOKENS = 24;

// Lenient on purpose. This stands in for a provider, and a provider that
// rejected an unknown field would fail the gateway for forwarding something
// harmless. The fields below are the ones the mock's behaviour depends on.
const RequestSchema = z.object({
  model: z.string().min(1).default('mock-1'),
  messages: z
    .array(z.object({ role: z.string(), content: z.unknown() }).loose())
    .min(1),
  stream: z.boolean().default(false),
  stream_options: z.object({ include_usage: z.boolean().optional() }).loose().optional(),
  max_tokens: z.number().int().positive().optional(),
  response_format: z.unknown().optional(),
  tools: z.array(z.unknown()).optional(),
  include_obfuscation: z.boolean().optional(),
});

export interface MockOptions {
  logLevel?: string;
  /** Default dialect; a request may override it with `x-mock-dialect`. */
  dialect?: Dialect;
  /**
   * Applied when a request carries no `x-mock-fail` of its own.
   *
   * A gateway sends its own headers upstream, so a client cannot reach past it
   * to fault one provider in a chain. Configuring the instance is how a
   * failover test makes the first provider unhealthy and leaves the second
   * working.
   */
  alwaysFail?: string;
  /**
   * Emit each piece as two deltas, cutting words in half.
   *
   * Real providers do not all chunk on token boundaries, and a gateway cannot
   * choose how they chunk. With word-aligned deltas, counting each delta and
   * re-encoding the whole prefix give identical answers, so a test built only
   * on aligned output cannot tell a correct counter from a broken one.
   */
  splitDeltas?: boolean;
}

function intHeader(value: string | string[] | undefined, fallback: number): number {
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw === undefined) return fallback;
  const n = Number(raw);
  return Number.isInteger(n) && n >= 0 ? n : fallback;
}

function estimatePromptTokens(messages: Array<{ content: unknown }>): number {
  const text = messages.map((m) => (typeof m.content === 'string' ? m.content : '')).join(' ');
  return Math.max(1, Math.ceil(text.length / 4));
}

/**
 * Writes and waits for the chunk to reach the socket.
 *
 * Awaiting the callback rather than only the return value matters here: several
 * failure modes write a frame and then kill the connection, and destroying a
 * socket with the write still buffered discards the very bytes the test is
 * about. It also gives correct backpressure, since the callback does not fire
 * until the buffer drains.
 */
function write(res: ServerResponse, chunk: string): Promise<void> {
  return new Promise((resolve, reject) => {
    res.write(chunk, (err) => (err ? reject(err) : resolve()));
  });
}

export function buildMockServer(options: MockOptions = {}): FastifyInstance {
  const app = Fastify({ logger: { level: options.logLevel ?? 'silent' } });

  app.post('/v1/chat/completions', async (request, reply) => {
    const parsed = RequestSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply
        .code(400)
        .send({ error: { message: parsed.error.issues[0]?.message ?? 'invalid request', type: 'invalid_request_error', code: null } });
    }
    const body = parsed.data;

    // parseFailure throws on an unrecognised mode, which surfaces as a 400 the
    // test sees rather than a silently healthy stream it does not.
    const failHeader = Array.isArray(request.headers['x-mock-fail'])
      ? request.headers['x-mock-fail'][0]
      : request.headers['x-mock-fail'];

    let failure: Failure;
    try {
      failure = parseFailure(failHeader ?? options.alwaysFail);
    } catch (err) {
      return reply.code(400).send({
        error: { message: (err as Error).message, type: 'invalid_request_error', code: null },
      });
    }

    const dialectHeader = request.headers['x-mock-dialect'];
    const dialect: Dialect =
      (Array.isArray(dialectHeader) ? dialectHeader[0] : dialectHeader) === 'groq'
        ? 'groq'
        : (options.dialect ?? 'openai');

    const seed = intHeader(request.headers['x-mock-seed'], 1);
    const tokenCount = intHeader(request.headers['x-mock-tokens'], DEFAULT_TOKENS);

    // Groq rejects a structured-output request combined with streaming. Worth
    // reproducing: it is a 400 the gateway must classify as fatal rather than
    // failing over, since every provider would reject the same body.
    if (dialect === 'groq' && body.stream && body.response_format !== undefined) {
      return reply.code(400).send({
        error: {
          message: 'response_format is not supported with streaming',
          type: 'invalid_request_error',
          code: null,
        },
      });
    }

    if (failure.kind === 'bad-request') {
      return reply.code(400).send({
        error: {
          message: 'unsupported value for parameter model',
          type: 'invalid_request_error',
          code: 'model_not_found',
        },
      });
    }

    if (failure.kind === 'server-error') {
      return reply.code(500).send({
        error: { message: 'internal server error', type: 'server_error', code: null },
      });
    }

    if (failure.kind === 'rate-limit') {
      return reply
        .code(429)
        // Go duration strings, not seconds. parseInt('2m59.56s') is 2, which is
        // the bug this header exists to catch.
        .headers({
          'x-ratelimit-limit-requests': '14400',
          'x-ratelimit-remaining-requests': '0',
          'x-ratelimit-reset-requests': '2m59.56s',
          'x-ratelimit-limit-tokens': '18000',
          'x-ratelimit-remaining-tokens': '0',
          'x-ratelimit-reset-tokens': '7.66s',
          'retry-after': '180',
        })
        .send({
          error: { message: 'Rate limit reached for model', type: 'rate_limit_exceeded' },
        });
    }

    const opts: FrameOptions = {
      id: completionId(seed),
      created: Math.floor(Date.now() / 1000),
      model: body.model,
      dialect,
      obfuscation: body.include_obfuscation !== false,
      crlf: request.headers['x-mock-crlf'] !== undefined,
    };

    const whole = [...contentPieces(seed, tokenCount)];
    const pieces =
      options.splitDeltas === true
        ? whole.flatMap((piece) => {
            // Split by code point, not by UTF-16 index, or the halves cut an
            // astral character in two and the text no longer round-trips.
            const points = [...piece];
            const at = Math.max(1, Math.floor(points.length / 2));
            return [points.slice(0, at).join(''), points.slice(at).join('')].filter(
              (part) => part !== '',
            );
          })
        : whole;
    const usage: Usage = {
      prompt_tokens: estimatePromptTokens(body.messages),
      completion_tokens: pieces.length,
      total_tokens: estimatePromptTokens(body.messages) + pieces.length,
    };

    if (!body.stream) {
      if (failure.kind === 'hang') {
        reply.hijack();
        return; // headers never sent; only a client-side timeout ends this
      }

      // Connection-level failures are not a streaming concern. A non-streaming
      // call can have its socket dropped just as easily, and a mock that only
      // broke streams would let the gateway's non-streaming error handling go
      // untested.
      if (failure.kind === 'abort' || failure.kind === 'fin') {
        reply.hijack();
        if (failure.kind === 'abort') reply.raw.destroy();
        else reply.raw.socket?.end();
        return;
      }

      if (failure.kind === 'truncate-json') {
        reply.hijack();
        reply.raw.writeHead(200, { 'content-type': 'application/json' });
        await new Promise<void>((resolve) => {
          reply.raw.write('{"id":"chatcmpl-', () => resolve());
        });
        reply.raw.socket?.end();
        return;
      }
      const finishReason =
        failure.kind === 'length-stop' ? 'length'
        : failure.kind === 'filter-stop' ? 'content_filter'
        : 'stop';
      return reply
        .header('content-type', 'application/json')
        .send(completionBody(opts, pieces.join(''), usage, finishReason));
    }

    reply.hijack();
    const res = reply.raw;
    await streamResponse(res, opts, pieces, usage, failure, {
      includeUsage: body.stream_options?.include_usage === true,
    });
    return;
  });

  return app;
}

async function streamResponse(
  res: ServerResponse,
  opts: FrameOptions,
  pieces: string[],
  usage: Usage,
  failure: Failure,
  { includeUsage }: { includeUsage: boolean },
): Promise<void> {
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    // no-transform is what stops a compression layer from buffering the stream
    // into one burst, which no unit test would ever notice.
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    'x-accel-buffering': 'no',
  });

  // Once the client is gone there is nothing to write to, and a timer left
  // running would keep the process alive after the test finished.
  let clientGone = false;
  res.on('close', () => {
    clientGone = true;
  });

  try {
    await write(res, roleFrame(opts));

    for (const [index, piece] of pieces.entries()) {
      if (clientGone) return;
      if (failure.at === index && (await injectAt(res, opts, failure))) return;

      if (failure.kind === 'slow' && index > 0) await delay(failure.at);
      if (failure.kind === 'noise' && index % 4 === 3) await write(res, noiseFrame(opts));

      await write(res, contentFrame(opts, piece));
    }

    if (clientGone) return;
    // A failure index at or past the end still has to fire, or `abort@999`
    // would quietly produce a healthy stream.
    if (failure.at >= pieces.length && (await injectAt(res, opts, failure))) return;

    if (failure.kind === 'tool-split') {
      await writeSplitToolCall(res, opts);
    }

    await write(res, finishFrame(opts, 'stop'));
    if (includeUsage && failure.kind !== 'no-usage') {
      await write(res, usageFrame(opts, usage));
    }
    await write(res, doneFrame(opts));
    res.end();
  } catch {
    // The client hung up mid-write. Nothing to report to and nothing to clean.
    res.destroy();
  }
}

/**
 * Applies the failure. Returns true when the response is finished and the
 * caller must stop writing.
 */
async function injectAt(
  res: ServerResponse,
  opts: FrameOptions,
  failure: Failure,
): Promise<boolean> {
  switch (failure.kind) {
    case 'abort':
      res.destroy();
      return true;

    case 'fin':
      res.socket?.end();
      return true;

    case 'clean-end':
      // A correct HTTP end with an incomplete answer. No error is raised and no
      // timeout fires, so only a missing terminal frame reveals it.
      res.end();
      return true;

    // The next three write something and then go away. They close with FIN
    // rather than RST: a reset can discard bytes still in flight, and the bytes
    // are the whole point of these modes. The stream still ends with no
    // terminal frame, which is what the gateway has to detect.
    case 'truncate-json':
      await write(res, 'data: {"id":"chatcmpl-');
      res.socket?.end();
      return true;

    case 'error-data':
      await write(res, errorDataFrame(opts, 'the model is overloaded'));
      res.socket?.end();
      return true;

    case 'error-event':
      await write(res, errorEventFrame(opts, 'the model is overloaded'));
      res.socket?.end();
      return true;

    case 'x-groq-error':
      await write(res, groqErrorFrame(opts, 'generation stopped early'));
      await write(res, doneFrame(opts));
      res.end();
      return true;

    case 'filter-stop':
      await write(res, finishFrame(opts, 'content_filter'));
      await write(res, doneFrame(opts));
      res.end();
      return true;

    case 'length-stop':
      await write(res, finishFrame(opts, 'length'));
      await write(res, doneFrame(opts));
      res.end();
      return true;

    case 'hang':
      // Deliberately never ends. Only an inter-chunk idle timer catches this,
      // which is a different timer from the one guarding headers.
      return true;

    default:
      return false;
  }
}

/**
 * Tool call arguments split across frames mid-escape and mid-UTF-8. An
 * accumulator that decodes or parses each delta on its own corrupts both.
 */
async function writeSplitToolCall(res: ServerResponse, opts: FrameOptions): Promise<void> {
  const parts = ['{"path":"C:\\', '\\Users\\\\me","note":"caf', '\u00e9 \u65e5', '\u672c"}'];
  for (const [i, part] of parts.entries()) {
    const delta =
      i === 0
        ? {
            tool_calls: [
              { index: 0, id: 'call_1', type: 'function', function: { name: 'read', arguments: part } },
            ],
          }
        : { tool_calls: [{ index: 0, function: { arguments: part } }] };
    await write(
      res,
      `data: ${JSON.stringify({
        id: opts.id,
        object: 'chat.completion.chunk',
        created: opts.created,
        model: opts.model,
        choices: [{ index: 0, delta, finish_reason: null }],
      })}${opts.crlf ? '\r\n\r\n' : '\n\n'}`,
    );
  }
}
