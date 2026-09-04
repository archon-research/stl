import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';

import { buildChainLabelLookup, type ChainLabelLookup } from '../lib/dashboard';
import { chainsQuery, protocolsQuery, tokenSymbolsQuery } from '../lib/queries';
import type { LocalChainRow, LocalProtocolRow } from '../types/local-data';

// Shared fallbacks for a registry that has not answered yet. A literal `?? []`
// would hand every `useMemo` downstream a fresh array on each render, which is
// the identity it compares on.
const NO_CHAINS: LocalChainRow[] = [];
const NO_PROTOCOLS: LocalProtocolRow[] = [];
const NO_TOKEN_SYMBOLS: string[] = [];

/**
 * Where a registry's request has got to, independent of what came back.
 *
 * The rows alone cannot say: a registry that has not answered, one that
 * answered 500 and one that answered `200 []` are all an empty array here, and
 * the three mean entirely different things to whoever reads them.
 */
export type RegistryStatus = {
  isPending: boolean;
  isError: boolean;
};

/** A registry's rows, and where the request behind them has got to. */
export type RegistryRead<Row> = RegistryStatus & {
  rows: Row[];
};

/**
 * Whether the rows are the registry's whole list, and so may be read as the set
 * of options that exist — a settled success speaks for the registry, including
 * when it speaks with an empty array.
 *
 * A pending registry has no list yet and a failed one has none it can vouch
 * for; treating either as the list is how a selection gets discarded for not
 * appearing in options that were never fetched.
 */
export function hasCompleteRows(status: RegistryStatus): boolean {
  return !status.isPending && !status.isError;
}

/**
 * The three registries every view reads, and the chain-label lookup built from
 * one of them. Each is its own hook rather than one bundle so a caller that
 * needs a single registry does not re-render on another's arrival; react-query
 * makes calling them from several places a cache read, not a second request.
 */
export function useLocalChains(): RegistryRead<LocalChainRow> {
  const { data, isPending, isError } = useQuery(chainsQuery());

  return { rows: data ?? NO_CHAINS, isPending, isError };
}

export function useLocalProtocols(): RegistryRead<LocalProtocolRow> {
  const { data, isPending, isError } = useQuery(protocolsQuery());

  return { rows: data ?? NO_PROTOCOLS, isPending, isError };
}

export function useTokenSymbols(): RegistryRead<string> {
  const { data, isPending, isError } = useQuery(tokenSymbolsQuery());

  return { rows: data ?? NO_TOKEN_SYMBOLS, isPending, isError };
}

// No error channel of its own: an unresolved chain falls back to its own id,
// which is a weaker label rather than a false one.
export function useChainLabels(): ChainLabelLookup {
  const { rows } = useLocalChains();

  return useMemo(() => buildChainLabelLookup(rows), [rows]);
}
