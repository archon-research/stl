import { useCallback, useEffect, useState } from 'react';

const URL_PARAMS_EVENT = 'stl:url-params-change';

export const PARAMS = {
  star: 'star',
  network: 'network',
  protocol: 'protocol',
  tab: 'tab',
  receiptToken: 'rt',
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

    window.addEventListener('popstate', syncFromLocation);
    window.addEventListener(URL_PARAMS_EVENT, syncFromLocation);

    return () => {
      window.removeEventListener('popstate', syncFromLocation);
      window.removeEventListener(URL_PARAMS_EVENT, syncFromLocation);
    };
  }, [key]);

  return [value, updateValue];
}
