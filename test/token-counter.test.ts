import { describe, expect, it } from 'vitest';
import type { ChatRequest } from '../src/providers/profile.js';
import { estimatePromptTokens } from '../src/tokens/counter.js';

describe('prompt estimates', () => {
  it('stays within two tokens of the measured Groq fixtures', () => {
    const fixtures: Array<{ request: ChatRequest; providerTokens: number }> = [
      {
        request: {
          model: 'openai/gpt-oss-20b',
          messages: [{ role: 'user', content: 'Reply with the single word ready.' }],
        },
        providerTokens: 78,
      },
      {
        request: {
          model: 'openai/gpt-oss-20b',
          messages: [
            { role: 'system', content: 'Answer briefly.' },
            { role: 'user', content: 'Return the number seven.' },
          ],
        },
        providerTokens: 82,
      },
      {
        request: {
          model: 'openai/gpt-oss-20b',
          messages: [{ role: 'user', content: '\u65e5\u672c\u8a9e and three emoji \ud83d\ude42\ud83d\ude42\ud83d\ude42' }],
        },
        providerTokens: 79,
      },
      {
        request: {
          model: 'openai/gpt-oss-20b',
          messages: [
            { role: 'user', content: 'Remember the code word amber.' },
            { role: 'assistant', content: 'I will remember amber.' },
            { role: 'user', content: 'What was the code word?' },
          ],
        },
        providerTokens: 98,
      },
      {
        request: {
          model: 'openai/gpt-oss-20b',
          messages: [{ role: 'user', content: 'a'.repeat(127) }],
        },
        providerTokens: 88,
      },
    ];

    for (const fixture of fixtures) {
      expect(
        Math.abs(estimatePromptTokens(fixture.request) - fixture.providerTokens),
      ).toBeLessThanOrEqual(2);
    }
  });

  it('does not apply Groq evidence to an unmeasured model', () => {
    const request = {
      messages: [{ role: 'user', content: 'hello' }],
    };

    const measured = estimatePromptTokens({ ...request, model: 'openai/gpt-oss-20b' });
    const unmeasured = estimatePromptTokens({ ...request, model: 'openai/gpt-oss-120b' });

    expect(measured - unmeasured).toBe(64);
  });
});
