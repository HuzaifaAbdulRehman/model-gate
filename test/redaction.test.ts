import { describe, expect, it } from 'vitest';
import { Redactor, entropy } from '../src/audit/redact.js';

const PEPPER = 'p'.repeat(32);
const r = new Redactor({ pepper: PEPPER });

describe('credentials', () => {
  it('drops a key entirely, keeping no handle on it', () => {
    // Never an id for a credential. A stable id is a correlatable handle on a
    // value that is still live somewhere.
    const out = r.redact('use sk-abcT3BlbkF' + 'Jdefghijklmnop for the call'); // gitleaks:allow

    expect(out.text).toBe('use [REDACTED:api_key] for the call');
    expect(out.entries[0]?.id).toBeUndefined();
    expect(out.classes).toEqual(['api_key']);
  });

  it('removes the whole PEM block, not just its header', () => {
    // Redacting the header alone leaves the key material in the log under it.
    const pem = '-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA\nabc\n-----END RSA PRIVATE KEY-----';
    const out = r.redact(`here it is:\n${pem}\nthanks`);

    expect(out.text).toBe('here it is:\n[REDACTED:private_key]\nthanks');
    expect(out.text).not.toContain('MIIEowIBAAKCAQEA');
  });

  it('catches the other credential shapes', () => {
    // Every value below is fabricated, and AKIAIOSFODNN7EXAMPLE is AWS's own
    // documented placeholder. A test for a secret detector has to contain
    // secret-shaped strings, so each line carries the scanner's own per-line
    // waiver rather than the file being added to a blanket ignore list that
    // would go on hiding real leaks later.
    const cases: Array<[string, string]> = [
      ['eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N', 'jwt'], // gitleaks:allow
      ['AKIAIOSFODNN7EXAMPLE', 'api_key'], // gitleaks:allow
      ['ghp_016C64B0FC5C4A2B8F9E1D7A3B5C8D2E6F40', 'api_key'], // gitleaks:allow
      ['xoxb-1234567890-abcdefghijkl', 'api_key'], // gitleaks:allow
      ['sk_live_abcdefghijklmnop1234', 'api_key'], // gitleaks:allow
      ['postgres://user:hunter2@db.internal:5432/app', 'url_credentials'], // gitleaks:allow
    ];

    for (const [secret, klass] of cases) {
      const out = r.redact(`value ${secret} end`);
      expect(out.classes, secret).toContain(klass);
      expect(out.text, secret).not.toContain(secret);
    }
  });

  it('does not redact ordinary prose that merely looks technical', () => {
    const text = 'the sk of the matter is that gh is a cli and xox is a hug';
    expect(r.redact(text).text).toBe(text);
  });
});

describe('personal data', () => {
  it('replaces an email with a typed placeholder and a stable id', () => {
    const out = r.redact('write to ada@example.com please');

    expect(out.text).toMatch(/^write to \[REDACTED:email:[0-9a-f]{8}\] please$/);
    expect(out.entries[0]?.c).toBe('email');
  });

  it('gives the same value the same id, and different peppers different ids', () => {
    // Correlating two occurrences without storing the value is the entire point
    // of the id. Sharing it across deployments is not.
    const a = r.redact('ada@example.com');
    const b = r.redact('ADA@example.com  ');
    const other = new Redactor({ pepper: 'q'.repeat(32) }).redact('ada@example.com');

    expect(a.entries[0]?.id).toBe(b.entries[0]?.id);
    expect(a.entries[0]?.id).not.toBe(other.entries[0]?.id);
  });

  it('finds phone numbers in several formats', () => {
    const out = r.redact('call +92 300 1234567 or +1 415 555 2671');

    expect(out.classes).toContain('phone');
    expect(out.text).not.toContain('1234567');
    expect(out.entries.filter((e) => e.c === 'phone')).toHaveLength(2);
  });

  it('leaves a version string alone', () => {
    // A pattern loose enough for international phone formats also matches every
    // long number, which is why this goes through a real parser.
    const text = 'upgrade to version 1.2.3 build 4567 today';
    expect(r.redact(text).text).toBe(text);
  });

  it('redacts a dashed SSN and a CNIC but not a bare digit run', () => {
    expect(r.redact('ssn 123-45-6789').classes).toContain('national_id');
    expect(r.redact('cnic 42101-1234567-8').classes).toContain('national_id');
    // Nine digits in prose is far more often an order number.
    expect(r.redact('order 123456789 shipped').text).toBe('order 123456789 shipped');
  });
});

describe('credit cards', () => {
  it('redacts a real card and keeps the brand and last four', () => {
    const out = r.redact('card 4242 4242 4242 4242 expires soon');

    expect(out.text).toBe('card [REDACTED:credit_card:visa:4242] expires soon');
  });

  it('requires an issuer prefix, not just a passing checksum', () => {
    // Luhn alone passes roughly one in ten random sixteen-digit strings, so a
    // checksum-only rule deletes order numbers out of the prompts this log
    // exists to help debug.
    const luhnValid = '9999999999999995';
    expect(r.redact(`ref ${luhnValid} ok`).classes).not.toContain('credit_card');
  });

  it('ignores a number that fails the checksum', () => {
    expect(r.redact('card 4242 4242 4242 4243').classes).not.toContain('credit_card');
  });
});

describe('ip addresses', () => {
  it('redacts a routable address', () => {
    expect(r.redact('from 203.0.113.7').classes).toContain('ip_address');
  });

  it('leaves loopback, the unspecified address and broadcast alone', () => {
    for (const ip of ['127.0.0.1', '0.0.0.0', '255.255.255.255']) {
      expect(r.redact(`host ${ip}`).classes, ip).not.toContain('ip_address');
    }
  });

  it('does not treat a semver as an address', () => {
    expect(r.redact('release 10.400.2.1').classes).not.toContain('ip_address');
  });
});

describe('entropy', () => {
  it('flags without redacting', () => {
    // In prompt text a false positive silently destroys the content someone
    // needed to read, so this raises a flag and leaves the text alone.
    const blob = 'Zm9vYmFyYmF6cXV4Y29ycmdlZ3JhdWx0Z2FycGx5' + 'X9vK3qL7wZ2mN8pT';
    const out = r.redact(`payload ${blob} end`);

    expect(out.entropySuspect).toBe(true);
    expect(out.text).toContain(blob);
  });

  it('does not flag ordinary english, a uuid or a git sha', () => {
    // Gitleaks' 3.5 bits threshold fires on all three. Prose measures about
    // 4.05 bits per character, which is why the gate here is higher and
    // requires a credential-shaped token.
    const samples = [
      'the quick brown fox jumps over the lazy dog and keeps on running along',
      'id 3f2504e0-4f89-11d3-9a0c-0305e82c3301 was created',
      'commit 9fceb02d0ae598e95dc970b74767f19372d61af8 landed',
    ];
    for (const s of samples) expect(r.redact(s).entropySuspect, s).toBe(false);
  });

  it('measures entropy the usual way', () => {
    expect(entropy('aaaa')).toBe(0);
    expect(entropy('ab')).toBe(1);
  });
});

describe('the manifest', () => {
  it('offsets point into the redacted text, not the original', () => {
    // They have to line up with the placeholder, or the manifest describes a
    // string that no longer exists.
    const out = r.redact('hi ada@example.com bye');
    const entry = out.entries[0];

    expect(entry).toBeDefined();
    expect(out.text.slice(entry?.off ?? 0)).toMatch(/^\[REDACTED:email:/);
    expect(entry?.orig_len).toBe('ada@example.com'.length);
  });

  it('never records the value it removed', () => {
    const out = r.redact('mail ada@example.com and card 4242424242424242');

    expect(JSON.stringify(out.entries)).not.toContain('ada@example.com');
    expect(JSON.stringify(out.entries)).not.toContain('4242424242424242');
  });

  it('replaces overlapping findings once', () => {
    // An email inside a credential must not be spliced twice, or every offset
    // after it is wrong.
    const out = r.redact('url postgres://ada@example.com:pw@host/db end');

    expect(out.text.match(/\[REDACTED/g)?.length).toBe(1);
  });

  it('handles several findings in one string in order', () => {
    const out = r.redact('ada@example.com then 203.0.113.7 then 4242424242424242');

    expect(out.entries.map((e) => e.c)).toEqual(['email', 'ip_address', 'credit_card']);
    for (const entry of out.entries) {
      expect(out.text.slice(entry.off)).toMatch(/^\[REDACTED:/);
    }
  });

  it('leaves multi-byte text intact around a redaction', () => {
    const out = r.redact('日本語 ada@example.com 🙂');

    expect(out.text.startsWith('日本語 ')).toBe(true);
    expect(out.text.endsWith(' 🙂')).toBe(true);
  });
});

describe('construction', () => {
  it('refuses a pepper too short to be worth having', () => {
    expect(() => new Redactor({ pepper: 'short' })).toThrow(/pepper/);
  });
});
