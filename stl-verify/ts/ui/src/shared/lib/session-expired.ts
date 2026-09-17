import { isHttpRequestError } from '@archon-research/http-client-react';

// Set once per navigation; a second reload inside this window means the 401
// is the API's own and persistent, so the error must render instead.
const RELOAD_GUARD_KEY = 'stl:session-reload-at';
const RELOAD_GUARD_MS = 30_000;

export type SessionReloadDeps = {
  now: () => number;
  href: () => string;
  navigate: (href: string) => void;
  storage: Pick<Storage, 'getItem' | 'setItem'> | null;
};

/**
 * The edge answers an API call whose session has ended with 401; only a
 * document request gets the login redirect. Reloading the current URL turns the
 * one into the other.
 */
export function isSessionExpired(error: unknown): boolean {
  return isHttpRequestError(error) && error.status === 401;
}

function lastReloadAt(storage: SessionReloadDeps['storage']): number {
  try {
    return Number(storage?.getItem(RELOAD_GUARD_KEY) ?? 0);
  } catch {
    return 0;
  }
}

function markReload(storage: SessionReloadDeps['storage'], now: number): void {
  try {
    storage?.setItem(RELOAD_GUARD_KEY, String(now));
  } catch {
    // No storage means no loop guard; a single reload is still the right call.
  }
}

/** Reloads the page for the edge to redirect to login; false when guarded. */
export function reloadForLogin(deps: SessionReloadDeps): boolean {
  const now = deps.now();
  if (now - lastReloadAt(deps.storage) < RELOAD_GUARD_MS) {
    return false;
  }
  markReload(deps.storage, now);
  deps.navigate(deps.href());
  return true;
}

function safeSessionStorage(): SessionReloadDeps['storage'] {
  try {
    return globalThis.sessionStorage ?? null;
  } catch {
    return null;
  }
}

/** The real browser, or null where there is no document to reload (tests, SSR). */
export function browserSessionReloadDeps(): SessionReloadDeps | null {
  if (typeof globalThis.location === 'undefined') {
    return null;
  }
  return {
    now: () => Date.now(),
    href: () => globalThis.location.href,
    navigate: (href) => globalThis.location.assign(href),
    storage: safeSessionStorage(),
  };
}
