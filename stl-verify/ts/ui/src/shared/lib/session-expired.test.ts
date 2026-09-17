import { HttpRequestError } from '@archon-research/http-client-react';
import { describe, expect, it, type Mock, vi } from 'vitest';

import {
  isSessionExpired,
  reloadForLogin,
  type SessionReloadDeps,
} from './session-expired';

const httpError = (status: number) =>
  new HttpRequestError({
    method: 'get',
    path: '/v1/primes',
    body: 'OAuth flow failed.',
    response: new Response(null, { status }),
  });

function memoryStorage(): NonNullable<SessionReloadDeps['storage']> {
  const values = new Map<string, string>();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => {
      values.set(key, value);
    },
  };
}

function deps(
  overrides: Partial<Omit<SessionReloadDeps, 'navigate'>> = {},
): SessionReloadDeps & { navigate: Mock<(href: string) => void> } {
  return {
    now: () => 1_000_000,
    href: () => 'https://edge.example/allocation',
    storage: memoryStorage(),
    ...overrides,
    navigate: vi.fn<(href: string) => void>(),
  };
}

describe('isSessionExpired', () => {
  it('is a 401 from the request layer', () => {
    expect(isSessionExpired(httpError(401))).toBe(true);
  });

  it.each([403, 404, 500])('is not a %i', (status) => {
    expect(isSessionExpired(httpError(status))).toBe(false);
  });

  it('is not a non-HTTP failure', () => {
    expect(isSessionExpired(new Error('boom'))).toBe(false);
  });
});

describe('reloadForLogin', () => {
  it('reloads the current URL and records when', () => {
    const d = deps();
    expect(reloadForLogin(d)).toBe(true);
    expect(d.navigate).toHaveBeenCalledWith('https://edge.example/allocation');
    expect(d.storage?.getItem('stl:session-reload-at')).toBe('1000000');
  });

  it('refuses a second reload inside the guard window', () => {
    const storage = memoryStorage();
    expect(reloadForLogin(deps({ storage }))).toBe(true);
    const second = deps({ storage, now: () => 1_000_000 + 29_999 });
    expect(reloadForLogin(second)).toBe(false);
    expect(second.navigate).not.toHaveBeenCalled();
  });

  it('reloads again once the guard window has passed', () => {
    const storage = memoryStorage();
    reloadForLogin(deps({ storage }));
    const later = deps({ storage, now: () => 1_000_000 + 30_000 });
    expect(reloadForLogin(later)).toBe(true);
    expect(later.navigate).toHaveBeenCalledTimes(1);
  });

  it('still reloads when storage throws', () => {
    const throwing: SessionReloadDeps['storage'] = {
      getItem: () => {
        throw new Error('blocked');
      },
      setItem: () => {
        throw new Error('blocked');
      },
    };
    const d = deps({ storage: throwing });
    expect(reloadForLogin(d)).toBe(true);
    expect(d.navigate).toHaveBeenCalledTimes(1);
  });
});
