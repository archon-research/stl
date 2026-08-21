/**
 * The registry endpoints: chains, protocols, primes, tokens, prices, data
 * sources. Small unfiltered lists apart from `/v1/tokens`, which the token
 * picker calls with `chain_id`, `symbol` and `limit`.
 */
import { mockDelay } from '@archon-research/http-client-msw';
import type { MockHandler } from '@archon-research/http-client-msw';

import { SECOND_MS, isoAgo, mockNow } from '../clock.ts';
import {
  CHAINS,
  DATA_SOURCES,
  PRIMES,
  PROTOCOLS,
  provenanceAvailability,
  TOKENS,
  TOKEN_PRICES_USD,
} from '../fixtures/registry.ts';
import { LIST_DELAY_MS, mock } from '../mock-api.ts';
import { notFound, problemResponse } from '../problem.ts';
import {
  includesInsensitive,
  readChainId,
  readLimit,
  sameHex,
} from '../query.ts';
import type { Token, TokenPrice } from '../schema.ts';

const TOKEN_LIMIT_DEFAULT = 100;
const TOKEN_LIMIT_MAX = 500;
/** Prices come off a 15-minute off-chain poll, so a fresh quote is minutes old. */
const QUOTE_AGE_SECONDS = 336;

function findToken(chainId: string, address: string): Token | undefined {
  return TOKENS.find(
    (token) =>
      token.chain_id === Number(chainId) && sameHex(token.address, address),
  );
}

/**
 * A token's oracle quote, or the documented no-quote shape. The endpoint answers
 * `200` for a known token even when nothing has priced it, which is the branch
 * the UI's "unpriced" and "stale" badges hang off.
 */
function priceFor(token: Token, nowMs: number): TokenPrice {
  const priceUsd = TOKEN_PRICES_USD[token.id];

  if (priceUsd === undefined) {
    return {
      token_id: token.id,
      price_usd: null,
      source_type: null,
      source_id: null,
      source_name: null,
      source_display_name: null,
      timestamp: null,
      staleness_seconds: null,
      is_stale: true,
      staleness_reason: 'missing_quote',
    };
  }

  return {
    token_id: token.id,
    price_usd: priceUsd,
    source_type: 'offchain',
    source_id: 1,
    source_name: 'coingecko',
    source_display_name: 'CoinGecko',
    timestamp: isoAgo(nowMs, QUOTE_AGE_SECONDS * SECOND_MS),
    staleness_seconds: QUOTE_AGE_SECONDS,
    is_stale: false,
    staleness_reason: null,
  };
}

export function registryHandlers(): MockHandler[] {
  return [
    mock.get('/v1/primes', async ({ response }) => {
      await mockDelay(LIST_DELAY_MS);
      return response(200).json([...PRIMES]);
    }),

    mock.get('/v1/chains', ({ response }) => response(200).json([...CHAINS])),

    mock.get('/v1/protocols', ({ response }) =>
      response(200).json([...PROTOCOLS]),
    ),

    mock.get('/v1/data-sources', ({ response }) =>
      response(200).json({ sources: [...DATA_SOURCES] }),
    ),

    mock.get('/v1/tokens', async ({ query, response }) => {
      await mockDelay(LIST_DELAY_MS);
      const chainId = readChainId(query.get('chain_id'));
      if (!chainId.ok) {
        return response.untyped(problemResponse(chainId.problem));
      }
      const limit = readLimit(
        query.get('limit'),
        TOKEN_LIMIT_DEFAULT,
        TOKEN_LIMIT_MAX,
      );
      if (!limit.ok) {
        return response.untyped(problemResponse(limit.problem));
      }
      const symbol = query.get('symbol');

      const matched = TOKENS.filter(
        (token) => chainId.value === null || token.chain_id === chainId.value,
      ).filter(
        (token) => symbol === null || includesInsensitive(token.symbol, symbol),
      );

      return response(200).json(matched.slice(0, limit.value));
    }),

    mock.get(
      '/v1/tokens/{chain_id}/{token_address}',
      ({ params, response }) => {
        const token = findToken(params.chain_id, params.token_address);

        return token
          ? response(200).json(token)
          : response.untyped(
              problemResponse(
                notFound(
                  `Token not found: ${params.token_address} on chain ${params.chain_id}`,
                ),
              ),
            );
      },
    ),

    mock.get('/v1/provenance/available', ({ response }) =>
      response(200).json(provenanceAvailability()),
    ),

    mock.get(
      '/v1/tokens/{chain_id}/{token_address}/price',
      ({ params, response }) => {
        const token = findToken(params.chain_id, params.token_address);

        return token
          ? response(200).json(priceFor(token, mockNow()))
          : response.untyped(
              problemResponse(
                notFound(
                  `Token not found: ${params.token_address} on chain ${params.chain_id}`,
                ),
              ),
            );
      },
    ),
  ];
}
