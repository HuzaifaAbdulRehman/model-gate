import type { Config, ProviderName } from '../config.js';
import { groqProfile, mockProfile, type ProviderProfile } from './profile.js';

/**
 * Turns the configured order into the profile chain the dispatcher walks.
 *
 * Two mock entries are two separate servers, not one server addressed twice.
 * Two API keys for the same provider would not be two providers either: rate
 * limits are per organisation, so both would run out together and the failover
 * would be theatre.
 */
export function buildProviderChain(config: Config): ProviderProfile[] {
  const timeouts = {
    headers: config.PROVIDER_HEADERS_TIMEOUT_MS,
    body: config.PROVIDER_BODY_TIMEOUT_MS,
  };

  return config.PROVIDER_ORDER.map((name: ProviderName) => {
    switch (name) {
      case 'mock-primary':
        return mockProfile('mock-primary', config.MOCK_PRIMARY_URL, timeouts);
      case 'mock-backup':
        return mockProfile('mock-backup', config.MOCK_BACKUP_URL, timeouts);
      case 'groq':
        return groqProfile(config.GROQ_BASE_URL, timeouts);
    }
  });
}

export function buildApiKeys(config: Config): Record<string, string | undefined> {
  return { groq: config.GROQ_API_KEY };
}
