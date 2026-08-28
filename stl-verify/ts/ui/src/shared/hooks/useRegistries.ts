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
 * The three registries every view reads, and the chain-label lookup built from
 * one of them. Each is its own hook rather than one bundle so a caller that
 * needs a single registry does not re-render on another's arrival; react-query
 * makes calling them from several places a cache read, not a second request.
 */
export function useLocalChains(): LocalChainRow[] {
  return useQuery(chainsQuery()).data ?? NO_CHAINS;
}

export function useLocalProtocols(): LocalProtocolRow[] {
  return useQuery(protocolsQuery()).data ?? NO_PROTOCOLS;
}

export function useTokenSymbols(): string[] {
  return useQuery(tokenSymbolsQuery()).data ?? NO_TOKEN_SYMBOLS;
}

export function useChainLabels(): ChainLabelLookup {
  const localChains = useLocalChains();

  return useMemo(() => buildChainLabelLookup(localChains), [localChains]);
}
