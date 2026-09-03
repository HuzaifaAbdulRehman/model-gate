/**
 * Deterministic token source for the mock provider.
 *
 * Phase 5 measures seam quality by killing the same response at a swept range
 * of offsets, which only means anything if the response is byte-identical every
 * time. A seeded generator gives that without recording fixtures.
 */

/** mulberry32. Small, fast, and good enough for reproducible test text. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Ordinary words, plus a few multi-byte ones. The non-ASCII entries are
// deliberate: a relay that decodes each TCP chunk independently corrupts them,
// and English-only fixtures hide that bug completely.
const WORDS = [
  'the', 'gateway', 'forwards', 'a', 'request', 'and', 'the', 'provider',
  'answers', 'with', 'tokens', 'that', 'arrive', 'one', 'piece', 'at',
  'a', 'time', 'which', 'is', 'why', 'buffering', 'the', 'whole',
  'response', 'defeats', 'the', 'point', 'of', 'streaming', 'entirely',
  'résumé', 'naïve', 'café', '日本語', 'emoji', '🙂', 'straße', 'Ω',
];

/**
 * Yields content pieces shaped like real deltas: a leading space on every piece
 * but the first, so concatenating them produces ordinary prose and the seam
 * logic in phase 5 has real word boundaries to find.
 */
export function* contentPieces(seed: number, count: number): Generator<string> {
  const rand = mulberry32(seed);
  for (let i = 0; i < count; i += 1) {
    const word = WORDS[Math.floor(rand() * WORDS.length)] ?? 'the';
    yield i === 0 ? word : ` ${word}`;
  }
}

/** The full completion for a seed, used to assert relayed text was not altered. */
export function completionFor(seed: number, count: number): string {
  return [...contentPieces(seed, count)].join('');
}

const BASE62 = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

/**
 * OpenAI's id shape, derived from the seed so a reproduced run reproduces the
 * id too. Every frame in one response carries the same one.
 */
export function completionId(seed: number): string {
  const rand = mulberry32(seed ^ 0x9e3779b9);
  let out = '';
  for (let i = 0; i < 22; i += 1) {
    out += BASE62[Math.floor(rand() * BASE62.length)] ?? 'a';
  }
  return `chatcmpl-${out}`;
}
