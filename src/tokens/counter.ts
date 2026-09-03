import { countTokens as countO200k } from 'gpt-tokenizer/encoding/o200k_base';
import { countTokens as countCl100k } from 'gpt-tokenizer/encoding/cl100k_base';
import type { ChatRequest } from '../providers/profile.js';

export type Encoding = 'o200k_base' | 'cl100k_base';

/**
 * Chat framing overhead, in tokens.
 *
 * These are the ChatML numbers. They are correct for the OpenAI 4o and 5 era
 * and they are NOT verified for gpt-oss harmony framing, which is what the Groq
 * free tier serves. Calibrating those properly needs ground truth from a live
 * provider: Groq returns an exact input count under `x_groq.debug.input_tokens`
 * when `debug: true` is set, so the honest fix is to fit the constants against
 * it and commit the fitted numbers with the date and method. Until then this is
 * an approximation, and the estimate error is a measured output of phase 6
 * rather than something to be quietly assumed away.
 */
const FRAMING = {
  /** Added once for the whole conversation. */
  bosOnce: 0,
  /** Per message, on top of its content and role. */
  perMessage: 3,
  /** Extra when a message carries a `name`. */
  perName: 1,
  /** The assistant turn the model is about to start. */
  replyPrimer: 3,
} as const;

/**
 * A completion ceiling used when the caller did not name one.
 *
 * Deliberately a small constant rather than the model's context window. Azure
 * infers the full context size in this situation, and a single request then
 * reserves a tenant's entire minute of budget.
 */
export const DEFAULT_MAX_TOKENS = 1_024;

function encodingFor(model: string): Encoding {
  // Everything current uses o200k. cl100k is kept for the older 3.5 and 4 line,
  // which a caller may still name.
  return /gpt-3\.5|gpt-4(?!\.|o)/.test(model) ? 'cl100k_base' : 'o200k_base';
}

export function countTokens(text: string, encoding: Encoding = 'o200k_base'): number {
  return encoding === 'cl100k_base' ? countCl100k(text) : countO200k(text);
}

/**
 * Loads the encoding tables before any request needs them, so the first call
 * does not pay roughly 200ms of table construction.
 */
export function warmUp(): void {
  countO200k('warm');
  countCl100k('warm');
}

function contentToText(content: unknown): string {
  if (typeof content === 'string') return content;
  // Multimodal content arrives as an array of parts. Only the text parts can be
  // counted this way; images are priced differently and are not estimated here.
  if (Array.isArray(content)) {
    return content
      .map((part) =>
        typeof part === 'object' && part !== null && typeof (part as { text?: unknown }).text === 'string'
          ? (part as { text: string }).text
          : '',
      )
      .join(' ');
  }
  return '';
}

/** Estimated prompt tokens, framing included. */
export function estimatePromptTokens(request: ChatRequest): number {
  const encoding = encodingFor(request.model);
  let total = FRAMING.bosOnce + FRAMING.replyPrimer;

  for (const message of request.messages) {
    total += FRAMING.perMessage;
    total += countTokens(String(message['role'] ?? ''), encoding);
    total += countTokens(contentToText(message['content']), encoding);
    if (typeof message['name'] === 'string') total += FRAMING.perName;
  }

  // Tool definitions are part of the prompt the model sees. Counting the JSON is
  // rough, and being roughly right beats counting zero.
  if (Array.isArray(request['tools']) && request['tools'].length > 0) {
    total += countTokens(JSON.stringify(request['tools']), encoding);
  }

  return total;
}

/**
 * What the limiter must hold before the request is allowed through.
 *
 * The true cost is unknown until the response ends, so the worst case is
 * reserved up front and the difference refunded afterwards. Reserving only the
 * prompt would let a tenant with almost no budget start a generation that
 * consumes far more than they had.
 *
 * Always an integer: the reservation is encoded into a Redis sorted-set member
 * and parsed back out with a digits-only pattern, and a fractional value would
 * fail that parse inside the Lua script rather than at the boundary.
 */
export function reservationFor(request: ChatRequest): { prompt: number; reserve: number } {
  const prompt = estimatePromptTokens(request);
  const completionCeiling = request.max_tokens ?? DEFAULT_MAX_TOKENS;
  return { prompt, reserve: Math.ceil(prompt + completionCeiling) };
}
