import { describe, expect, it } from 'vitest';
import { SseFramer, isDone, viewChunk } from '../src/streaming/framer.js';

const encoder = new TextEncoder();

function collect(framer: SseFramer, bytes: Uint8Array): string[] {
  return [...framer.push(bytes)].map((f) => f.raw);
}

/** Feeds the same bytes one at a time, which is the worst case a network gives. */
function collectByteAtATime(text: string): string[] {
  const framer = new SseFramer();
  const bytes = encoder.encode(text);
  const out: string[] = [];
  for (const byte of bytes) out.push(...collect(framer, new Uint8Array([byte])));
  const tail = framer.flush();
  if (tail !== null) out.push(tail.raw);
  return out;
}

const SAMPLE =
  'data: {"choices":[{"delta":{"content":"héllo"},"finish_reason":null}]}\n\n' +
  'data: {"choices":[{"delta":{"content":" 日本語"},"finish_reason":null}]}\n\n' +
  'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n' +
  'data: [DONE]\n\n';

describe('framing', () => {
  it('splits on a blank line', () => {
    const framer = new SseFramer();
    const frames = [...framer.pushText(SAMPLE)];

    expect(frames).toHaveLength(4);
    expect(frames[3]?.data).toEqual(['[DONE]']);
  });

  it('splits on CRLF as well', () => {
    const framer = new SseFramer();
    const frames = [...framer.pushText(SAMPLE.replace(/\n\n/g, '\r\n\r\n'))];

    expect(frames).toHaveLength(4);
  });

  it('produces identical frames when fed one byte at a time', () => {
    // The test that fails the moment someone decodes each chunk on its own or
    // splits on a fixed byte count. Measured on loopback, roughly one upstream
    // chunk in a hundred and fifty does not end on a frame boundary, and over a
    // real network it is worse.
    const whole = new SseFramer();
    const atOnce = [...whole.pushText(SAMPLE)].map((f) => f.raw);

    expect(collectByteAtATime(SAMPLE)).toEqual(atOnce);
  });

  it('keeps a multi-byte character whole at every possible split point', () => {
    // A decoder created per chunk turns the halves into replacement characters.
    // Splitting at one chosen offset is not enough to prove anything: a
    // midpoint often lands in the ASCII prefix and never bisects a character
    // at all, so the broken version passes. Sweeping every offset cannot miss.
    const text = 'data: {"c":"日本語 🙂 café"}\n\n';
    const bytes = encoder.encode(text);

    for (let split = 1; split < bytes.length; split += 1) {
      const framer = new SseFramer();
      const frames = [
        ...framer.push(bytes.slice(0, split)),
        ...framer.push(bytes.slice(split)),
      ];

      expect(frames, `split at ${split}`).toHaveLength(1);
      expect(frames[0]?.raw, `split at ${split}`).toBe(text);
      expect(frames[0]?.raw, `split at ${split}`).not.toContain('�');
    }
  });

  it('forwards the original bytes rather than a re-encoding', () => {
    // Re-serialising from the parsed form drops fields the provider added and
    // changes the bytes for no reason.
    const framer = new SseFramer();
    const raw = 'data: {"z":1,"a":2,"obfuscation":"xxxx"}\n\n';
    const frames = [...framer.pushText(raw)];

    expect(frames[0]?.raw).toBe(raw);
  });

  it('holds an incomplete frame instead of emitting half of one', () => {
    const framer = new SseFramer();
    const frames = [...framer.pushText('data: {"partial":')];

    expect(frames).toHaveLength(0);
    expect(framer.pending).toBe(true);
  });

  it('reports a torn frame on flush', () => {
    // Exactly what a mid-stream kill leaves behind, and often the last thing
    // the provider managed to say.
    const framer = new SseFramer();
    [...framer.pushText('data: {"id":"chatcmpl-')];

    expect(framer.flush()?.raw).toBe('data: {"id":"chatcmpl-');
    expect(framer.flush()).toBeNull();
  });

  it('reads a named event', () => {
    const framer = new SseFramer();
    const frames = [...framer.pushText('event: error\ndata: {"error":{"message":"x"}}\n\n')];

    expect(frames[0]?.event).toBe('error');
    expect(frames[0]?.data).toEqual(['{"error":{"message":"x"}}']);
  });

  it('keeps a comment keepalive but reads no fields from it', () => {
    const framer = new SseFramer();
    const frames = [...framer.pushText(': ping\n\n')];

    expect(frames).toHaveLength(1);
    expect(frames[0]?.data).toEqual([]);
  });

  it('strips exactly one space after the colon', () => {
    const framer = new SseFramer();
    const frames = [...framer.pushText('data:  leading\n\n')];

    expect(frames[0]?.data).toEqual([' leading']);
  });
});

describe('reading a chunk', () => {
  it('reports a content delta', () => {
    const framer = new SseFramer();
    const [frame] = [...framer.pushText('data: {"choices":[{"delta":{"content":"hi"}}]}\n\n')];

    expect(viewChunk(frame!)?.hasContent).toBe(true);
  });

  it('does not treat an empty delta as content', () => {
    // The opening role frame carries content:"" and must not commit the stream,
    // or the commit point fires before a single useful byte exists.
    const framer = new SseFramer();
    const [frame] = [
      ...framer.pushText('data: {"choices":[{"delta":{"role":"assistant","content":""}}]}\n\n'),
    ];

    expect(viewChunk(frame!)?.hasContent).toBe(false);
  });

  it('reads the finish reason', () => {
    const framer = new SseFramer();
    const [frame] = [...framer.pushText('data: {"choices":[{"delta":{},"finish_reason":"length"}]}\n\n')];

    expect(viewChunk(frame!)?.finishReason).toBe('length');
  });

  it('survives a frame carrying no choices at all', () => {
    // Real streams contain these. Reaching for choices[0] unconditionally
    // throws on the first one.
    const framer = new SseFramer();
    const [frame] = [...framer.pushText('data: {"choices":[],"usage":{"total_tokens":9}}\n\n')];

    expect(() => viewChunk(frame!)).not.toThrow();
    expect(viewChunk(frame!)?.hasContent).toBe(false);
  });

  it('reads an in-band error in either shape', () => {
    const framer = new SseFramer();
    const [openai] = [...framer.pushText('data: {"error":{"message":"overloaded"}}\n\n')];
    const [groq] = [...framer.pushText('data: {"choices":[],"x_groq":{"error":"stopped"}}\n\n')];

    expect(viewChunk(openai!)?.error?.message).toBe('overloaded');
    expect(viewChunk(groq!)?.error?.message).toBe('stopped');
  });

  it('returns null for the done sentinel and recognises it', () => {
    const framer = new SseFramer();
    const [frame] = [...framer.pushText('data: [DONE]\n\n')];

    expect(isDone(frame!)).toBe(true);
    expect(viewChunk(frame!)).toBeNull();
  });

  it('does not throw on a torn payload', () => {
    const framer = new SseFramer();
    const [frame] = [...framer.pushText('data: {"id":"cha\n\n')];

    expect(viewChunk(frame!)).toBeNull();
  });
});
