import { useCallback, useEffect, useState } from 'react';

const URL_PARAMS_EVENT = 'stl:url-params-change';
const URL_PATH_EVENT = 'stl:url-path-change';

export const PARAMS = {
  prime: 'prime',
  network: 'network',
  protocol: 'protocol',
  category: 'category',
  tab: 'tab',
  receiptToken: 'rt',
  activityAction: 'aa',
  showAllPrimes: 'allp',
  // Data table params (shared across tables)
  sort: 'sort',
  search: 'q',
  // Drawer state
  drawerOpen: 'drawer',
} as const;

function isBrowser(): boolean {
  return typeof window !== 'undefined';
}

function readParams(): URLSearchParams {
  return new URLSearchParams(isBrowser() ? window.location.search : '');
}

export function getParam(key: string): string | null {
  return readParams().get(key);
}

export function setParam(key: string, value: string | null): void {
  if (!isBrowser()) {
    return;
  }

  const params = readParams();

  if (value === null || value === '') {
    params.delete(key);
  } else {
    params.set(key, value);
  }

  const query = params.toString();
  const nextUrl = `${window.location.pathname}${query ? `?${query}` : ''}${window.location.hash}`;

  window.history.replaceState(window.history.state, '', nextUrl);
  window.dispatchEvent(new Event(URL_PARAMS_EVENT));
}

export function setPathname(
  pathname: string,
  mode: 'push' | 'replace' = 'push',
): void {
  if (!isBrowser()) {
    return;
  }

  const normalizedPathname = pathname.startsWith('/')
    ? pathname
    : `/${pathname}`;
  const nextUrl = `${normalizedPathname}${window.location.search}${window.location.hash}`;

  if (mode === 'replace') {
    window.history.replaceState(window.history.state, '', nextUrl);
  } else {
    window.history.pushState(window.history.state, '', nextUrl);
  }

  window.dispatchEvent(new Event(URL_PATH_EVENT));
}

export function useUrlParam(
  key: string,
): [string | null, (value: string | null) => void] {
  const [value, setValue] = useState<string | null>(() => getParam(key));

  const updateValue = useCallback(
    (nextValue: string | null) => {
      setParam(key, nextValue);
      setValue(nextValue);
    },
    [key],
  );

  useEffect(() => {
    if (!isBrowser()) {
      return;
    }

    const syncFromLocation = () => {
      setValue(getParam(key));
    };

    syncFromLocation();

    window.addEventListener('popstate', syncFromLocation);
    window.addEventListener(URL_PARAMS_EVENT, syncFromLocation);

    return () => {
      window.removeEventListener('popstate', syncFromLocation);
      window.removeEventListener(URL_PARAMS_EVENT, syncFromLocation);
    };
  }, [key]);

  return [value, updateValue];
}

export function usePathname(): [string, (pathname: string) => void] {
  const [pathname, setPathnameState] = useState<string>(() =>
    isBrowser() ? window.location.pathname : '/',
  );

  const updatePathname = useCallback((nextPathname: string) => {
    setPathname(nextPathname, 'push');
    setPathnameState(
      nextPathname.startsWith('/') ? nextPathname : `/${nextPathname}`,
    );
  }, []);

  useEffect(() => {
    if (!isBrowser()) {
      return;
    }

    const syncFromLocation = () => {
      setPathnameState(window.location.pathname);
    };

    syncFromLocation();

    window.addEventListener('popstate', syncFromLocation);
    window.addEventListener(URL_PATH_EVENT, syncFromLocation);

    return () => {
      window.removeEventListener('popstate', syncFromLocation);
      window.removeEventListener(URL_PATH_EVENT, syncFromLocation);
    };
  }, []);

  return [pathname, updatePathname];
}
