import { createHmac } from 'node:crypto';
import { findNumbers } from 'libphonenumber-js';

/**
 * Prompt redaction, with one choke point the type system enforces.
 *
 * The point of the branded type is that passing a raw prompt to anything that
 * persists or logs becomes a compile error rather than something a reviewer has
 * to notice. Every leak in this class of system is a second copy of the data
 * that someone forgot about, not a failure of the redactor itself.
 */

declare const brand: unique symbol;
/** A string that has been through `redact`. Nothing else can produce one. */
export type Redacted = string & { readonly [brand]: 'redacted' };

export type RedactionClass =
  | 'api_key'
  | 'private_key'
  | 'jwt'
  | 'url_credentials'
  | 'email'
  | 'phone'
  | 'credit_card'
  | 'national_id'
  | 'ip_address'
  | 'high_entropy';

export interface RedactionEntry {
  /** Class, kept short because these are stored as JSONB per request. */
  c: RedactionClass;
  /** Which detector fired, so a false positive can be traced to a rule. */
  d: string;
  /** Offset into the REDACTED text, so the entry and its placeholder line up. */
  off: number;
  /** Length of what was removed, which is sometimes the only clue left. */
  orig_len: number;
  /** Stable HMAC id, absent for credentials. */
  id?: string;
}

export interface RedactionResult {
  text: Redacted;
  entries: RedactionEntry[];
  classes: RedactionClass[];
  /**
   * Something looked like a secret but was not confidently one. The text is
   * left intact and the row is flagged instead: in prompt text a false positive
   * destroys the very thing the audit log exists to help debug.
   */
  entropySuspect: boolean;
}

/**
 * Bumped whenever a pattern changes, so a gap in what was caught is explainable
 * by which engine wrote the row rather than guessed at later.
 */
export const ENGINE_VERSION = 'redact-1';

interface Match {
  start: number;
  end: number;
  c: RedactionClass;
  d: string;
  value: string;
  /** Replaces the default placeholder, for the card's last four. */
  label?: string;
  /** Credentials never keep a correlatable handle. */
  noId?: boolean;
}

/**
 * Credential patterns anchored on invariants rather than on a prefix alone.
 *
 * A prefix is what a random string collides with; the interior marker is what
 * makes a match mean something. Precision matters more than recall here,
 * because a missed key is one leak while a false positive silently corrupts
 * every prompt containing an order number.
 */
const CREDENTIAL_PATTERNS: Array<{ d: string; c: RedactionClass; re: RegExp }> = [
  // The full block, not the header line: redacting only the header leaves the
  // key material sitting in the log underneath it.
  {
    d: 'pem.block',
    c: 'private_key',
    re: /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g,
  },
  { d: 'jwt', c: 'jwt', re: /\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/g },
  { d: 'url.credentials', c: 'url_credentials', re: /\b[a-z][a-z0-9+.-]*:\/\/[^\s/@:]+:[^\s/@]+@/gi },
  // T3BlbkFJ is the literal that appears inside every OpenAI key. Matching on
  // "sk-" alone would catch a great many things that are not keys.
  { d: 'openai.key', c: 'api_key', re: /\bsk-[A-Za-z0-9_-]*T3BlbkFJ[A-Za-z0-9_-]+/g },
  { d: 'anthropic.key', c: 'api_key', re: /\bsk-ant-api\d{2}-[A-Za-z0-9_-]{80,120}/g },
  // Length is a range on purpose: gitleaks ships no Groq rule, so the exact
  // length here is unverified and a hard length would silently stop matching.
  { d: 'groq.key', c: 'api_key', re: /\bgsk_[A-Za-z0-9]{40,60}\b/g },
  { d: 'aws.access_key', c: 'api_key', re: /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/g },
  { d: 'github.token', c: 'api_key', re: /\bgh[pousr]_[A-Za-z0-9]{36,255}\b/g },
  { d: 'slack.token', c: 'api_key', re: /\bxox[baprs]-[A-Za-z0-9-]{10,}\b/g },
  { d: 'stripe.key', c: 'api_key', re: /\b[rs]k_(?:live|test)_[A-Za-z0-9]{16,}\b/g },
];

const EMAIL_RE = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g;
const IPV4_RE = /\b(?:\d{1,3}\.){3}\d{1,3}\b/g;
/** Only the dashed form. A bare nine-digit run in prose is unusable as a signal. */
const SSN_RE = /\b\d{3}-\d{2}-\d{4}\b/g;
/** Pakistani national identity number. */
const CNIC_RE = /\b\d{5}-\d{7}-\d\b/g;
/**
 * Anchored to end on a digit. Written as a repeated `\d[ -]?` the match also
 * accepts a trailing separator, so a card followed by a space swallows that
 * space into the redaction: the surrounding text loses a character and the
 * recorded original length is one too long.
 */
const CARD_CANDIDATE_RE = /\b\d(?:[ -]?\d){12,18}\b/g;

function luhn(digits: string): boolean {
  let sum = 0;
  let double = false;
  for (let i = digits.length - 1; i >= 0; i -= 1) {
    let d = digits.charCodeAt(i) - 48;
    if (d < 0 || d > 9) return false;
    if (double) {
      d *= 2;
      if (d > 9) d -= 9;
    }
    sum += d;
    double = !double;
  }
  return sum % 10 === 0;
}

/**
 * Luhn alone passes roughly one in ten random digit strings, which would redact
 * order numbers and invoice references out of the prompts this log exists to
 * help debug. The issuer prefix is what turns a checksum into evidence.
 */
function cardBrand(digits: string): string | null {
  if (/^4\d{12}(\d{3})?(\d{3})?$/.test(digits)) return 'visa';
  if (/^(5[1-5]\d{14}|2(2[2-9]\d{12}|[3-6]\d{13}|7[01]\d{12}|720\d{12}))$/.test(digits)) return 'mastercard';
  if (/^3[47]\d{13}$/.test(digits)) return 'amex';
  if (/^(6011|65\d{2}|64[4-9]\d)\d{12}$/.test(digits)) return 'discover';
  if (/^3(0[0-5]|[68]\d)\d{11}$/.test(digits)) return 'diners';
  if (/^(?:2131|1800|35\d{3})\d{11}$/.test(digits)) return 'jcb';
  return null;
}

function usableIp(value: string): boolean {
  const parts = value.split('.');
  if (parts.length !== 4) return false;
  // Rejects semver and build numbers, which is what an unvalidated dotted-quad
  // regex mostly finds in prose.
  if (!parts.every((p) => p.length <= 3 && Number(p) <= 255)) return false;
  if (value === '0.0.0.0' || value === '255.255.255.255') return false;
  if (value.startsWith('127.')) return false;
  return true;
}

function collect(text: string): Match[] {
  const matches: Match[] = [];

  for (const { d, c, re } of CREDENTIAL_PATTERNS) {
    for (const m of text.matchAll(re)) {
      if (m.index === undefined) continue;
      matches.push({ start: m.index, end: m.index + m[0].length, c, d, value: m[0], noId: true });
    }
  }

  for (const m of text.matchAll(CARD_CANDIDATE_RE)) {
    if (m.index === undefined) continue;
    const digits = m[0].replace(/[ -]/g, '');
    if (digits.length < 13 || digits.length > 19) continue;
    if (!luhn(digits)) continue;
    const brandName = cardBrand(digits);
    if (brandName === null) continue;
    matches.push({
      start: m.index,
      end: m.index + m[0].length,
      c: 'credit_card',
      d: 'card.luhn+iin',
      value: digits,
      label: `${brandName}:${digits.slice(-4)}`,
    });
  }

  for (const m of text.matchAll(EMAIL_RE)) {
    if (m.index === undefined) continue;
    matches.push({ start: m.index, end: m.index + m[0].length, c: 'email', d: 'email.rfc', value: m[0] });
  }

  for (const m of text.matchAll(SSN_RE)) {
    if (m.index === undefined) continue;
    matches.push({ start: m.index, end: m.index + m[0].length, c: 'national_id', d: 'ssn.dashed', value: m[0] });
  }

  for (const m of text.matchAll(CNIC_RE)) {
    if (m.index === undefined) continue;
    matches.push({ start: m.index, end: m.index + m[0].length, c: 'national_id', d: 'cnic', value: m[0] });
  }

  for (const m of text.matchAll(IPV4_RE)) {
    if (m.index === undefined || !usableIp(m[0])) continue;
    matches.push({ start: m.index, end: m.index + m[0].length, c: 'ip_address', d: 'ipv4', value: m[0] });
  }

  // The one category where a library genuinely beats a regex: a pattern loose
  // enough to catch international formats also catches every long number.
  try {
    // v2 is required for the object form that reports offsets; without it the
    // call returns bare strings and there is nothing to splice out.
    for (const found of findNumbers(text, { defaultCountry: 'US', v2: true })) {
      matches.push({
        start: found.startsAt,
        end: found.endsAt,
        c: 'phone',
        d: 'phone.libphonenumber',
        value: found.number.number,
      });
    }
  } catch {
    // Never fail a request because a phone parser disliked the input. Missing a
    // phone number is a smaller harm than refusing to serve the caller.
  }

  return matches;
}

/**
 * Earlier match wins, and a longer one beats a shorter one starting at the same
 * place. Without this a card inside a URL, or an email inside a credential,
 * would be replaced twice and the offsets would stop meaning anything.
 */
function resolveOverlaps(matches: Match[]): Match[] {
  const sorted = [...matches].sort((a, b) => a.start - b.start || b.end - a.end);
  const kept: Match[] = [];
  let cursor = -1;
  for (const m of sorted) {
    if (m.start < cursor) continue;
    kept.push(m);
    cursor = m.end;
  }
  return kept;
}

/** Shannon entropy in bits per character. */
export function entropy(value: string): number {
  if (value.length === 0) return 0;
  const counts = new Map<string, number>();
  for (const ch of value) counts.set(ch, (counts.get(ch) ?? 0) + 1);
  let bits = 0;
  for (const n of counts.values()) {
    const p = n / value.length;
    bits -= p * Math.log2(p);
  }
  return bits;
}

/**
 * Flags, never redacts.
 *
 * Gitleaks' 3.5 bits threshold also fires on plain English, UUIDs and git
 * hashes. In source code a false positive costs a developer half a minute; in
 * prompt text it silently deletes the content someone needed to read.
 */
function looksHighEntropy(text: string): boolean {
  for (const token of text.split(/\s+/)) {
    if (token.length < 32) continue;
    if (!/^[A-Za-z0-9+/=_-]+$/.test(token)) continue;
    if (entropy(token) >= 4.5) return true;
  }
  return false;
}

export interface RedactorOptions {
  /**
   * Keyed, not a plain hash. Emails and phone numbers come from a small enough
   * space that an unkeyed digest is reversible by enumeration, and a shared
   * salt does not fix an enumerable domain. Lives outside the database, so a
   * dump alone cannot re-identify what was removed.
   */
  pepper: string;
}

export class Redactor {
  readonly #pepper: string;

  constructor(options: RedactorOptions) {
    if (options.pepper.length < 16) {
      throw new Error('redaction pepper must be at least 16 characters');
    }
    this.#pepper = options.pepper;
  }

  /**
   * Keyed digest of the raw text, taken before anything is removed.
   *
   * Redaction has already happened by the time a row is written, so this is the
   * only way left to tell whether two audit rows came from the same original
   * prompt. Computed here rather than in SQL: a pepper passed to pgcrypto ends
   * up in pg_stat_statements and the Postgres log.
   */
  fingerprint(raw: string): Buffer {
    return createHmac('sha256', this.#pepper).update(raw).digest();
  }

  #id(value: string, kind: RedactionClass): string {
    const normalized = kind === 'phone' ? value.replace(/\D/g, '') : value.trim().toLowerCase();
    return createHmac('sha256', this.#pepper).update(normalized).digest('hex').slice(0, 8);
  }

  redact(raw: string): RedactionResult {
    const matches = resolveOverlaps(collect(raw));
    const entries: RedactionEntry[] = [];

    let out = '';
    let cursor = 0;
    for (const m of matches) {
      out += raw.slice(cursor, m.start);

      const id = m.noId === true ? undefined : this.#id(m.value, m.c);
      const placeholder =
        m.label !== undefined
          ? `[REDACTED:${m.c}:${m.label}]`
          : id === undefined
            ? `[REDACTED:${m.c}]`
            : `[REDACTED:${m.c}:${id}]`;

      entries.push({
        c: m.c,
        d: m.d,
        off: out.length,
        orig_len: m.end - m.start,
        ...(id === undefined ? {} : { id }),
      });

      out += placeholder;
      cursor = m.end;
    }
    out += raw.slice(cursor);

    return {
      text: out as Redacted,
      entries,
      classes: [...new Set(entries.map((e) => e.c))],
      entropySuspect: looksHighEntropy(out),
    };
  }

  /** For text that is already known to contain nothing, such as an empty body. */
  static empty(): Redacted {
    return '' as Redacted;
  }
}
