import { createApiClient } from '@archon-research/http-client-react';

import type { paths } from '../generated/openapi-types';
import type {
  AllocationActivityResponse,
  AllocationsResponse,
  CapitalMetricsListResponse,
  DataSourcesResponse,
  PrimeDebtSnapshot,
  PrimesResponse,
  ProtocolEventsResponse,
  RiskBreakdown,
  Rrc,
  Token,
  TokenPrice,
  TokensResponse,
  TxProtocolEventsResponse,
} from '../types/allocation';
import type { LocalChainRow, LocalProtocolRow } from '../types/local-data';
import { isAbortError } from './errors';
import { logging } from './logging';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';
const apiClient = createApiClient<paths>(API_BASE_URL);

type ApiResult<TData, TError> = Promise<{
  data?: TData;
  error?: TError;
  response: Response;
}>;

function toErrorBody(error: unknown): string {
  if (typeof error === 'string') {
    return error;
  }

  if (error instanceof Error) {
    return error.message;
  }

  if (error === undefined || error === null) {
    return 'No response body.';
  }

  try {
    return JSON.stringify(error);
  } catch (stringifyError) {
    logging.error('Failed to stringify error body', {
      errorType: typeof error,
      errorConstructor: (error as Record<string, unknown>)?.constructor?.name,
      errorKeys: error ? Object.keys(error) : [],
      stringifyError,
    });
    return 'Unserializable error body.';
  }
}

async function requestData<TData, TError>(
  request: ApiResult<TData, TError>,
  label: string,
): Promise<TData> {
  const { data, error, response } = await request;

  if (!response.ok || data === undefined) {
    const errorMessage = `${label} failed (${response.status}): ${toErrorBody(error)}`;

    logging.error('API request failed', {
      label,
      status: response.status,
      statusText: response.statusText,
      url: response.url,
      error,
    });

    throw new Error(errorMessage);
  }

  return data;
}

export function getPrimes(signal?: AbortSignal): Promise<PrimesResponse> {
  return requestData(apiClient.GET('/v1/primes', { signal }), 'GET /v1/primes');
}

export function getChains(signal?: AbortSignal): Promise<LocalChainRow[]> {
  return requestData(apiClient.GET('/v1/chains', { signal }), 'GET /v1/chains');
}

export function getProtocols(
  signal?: AbortSignal,
): Promise<LocalProtocolRow[]> {
  return requestData(
    apiClient.GET('/v1/protocols', { signal }),
    'GET /v1/protocols',
  );
}

export function getAllocations(
  primeId: string,
  signal?: AbortSignal,
): Promise<AllocationsResponse> {
  return requestData(
    apiClient.GET('/v1/primes/{prime_id}/allocations', {
      params: { path: { prime_id: primeId } },
      signal,
    }),
    'GET /v1/primes/{prime_id}/allocations',
  );
}

export function getRiskBreakdown(
  receiptTokenId: number,
  signal?: AbortSignal,
): Promise<RiskBreakdown> {
  return requestData(
    apiClient.GET('/v1/risk/{receipt_token_id}/breakdown', {
      params: { path: { receipt_token_id: receiptTokenId } },
      signal,
    }),
    'GET /v1/risk/{receipt_token_id}/breakdown',
  );
}

export function getRrc(
  assetId: number,
  primeId: string,
  signal?: AbortSignal,
): Promise<Rrc> {
  return requestData(
    apiClient.GET('/v1/risk/rrc', {
      params: {
        query: { asset_id: assetId, prime_id: primeId },
      },
      signal,
    }),
    'GET /v1/risk/rrc',
  );
}

export function getAllocationActivity(
  filters?: {
    prime_id?: string;
    chain_id?: number;
    protocol_name?: string;
    action_type?: string;
    token_symbol?: string;
    tx_hash?: string;
    from_timestamp?: string;
    to_timestamp?: string;
    limit?: number;
  },
  signal?: AbortSignal,
): Promise<AllocationActivityResponse> {
  return requestData(
    apiClient.GET('/v1/allocations/activity', {
      params: { query: filters },
      signal,
    }),
    'GET /v1/allocations/activity',
  );
}

export function getCapitalMetrics(
  signal?: AbortSignal,
): Promise<CapitalMetricsListResponse> {
  const endpointPath = '/v1/capital-metrics';
  const endpointUrl = API_BASE_URL
    ? `${API_BASE_URL}${endpointPath}`
    : endpointPath;

  return fetch(endpointUrl, { signal })
    .then(async (response) => {
      if (!response.ok) {
        const responseText = await response.text().catch(() => '<no body>');
        throw new Error(
          `GET ${endpointPath} failed (${response.status}): ${responseText}`,
        );
      }

      return response.json() as Promise<CapitalMetricsListResponse>;
    })
    .catch((error: unknown) => {
      if (!isAbortError(error)) {
        logging.error('Failed to fetch capital metrics list', { error });
      }
      throw error;
    });
}

export function getDataSources(
  signal?: AbortSignal,
): Promise<DataSourcesResponse> {
  return requestData(
    apiClient.GET('/v1/data-sources', { signal }),
    'GET /v1/data-sources',
  );
}

export function getProtocolEvents(
  filters?: {
    tx_hash?: string;
    protocol_name?: string;
    limit?: number;
  },
  signal?: AbortSignal,
): Promise<ProtocolEventsResponse> {
  return requestData(
    apiClient.GET('/v1/protocol-events', {
      params: { query: filters },
      signal,
    }),
    'GET /v1/protocol-events',
  );
}

export function getTxProtocolEvents(
  txHash: string,
  signal?: AbortSignal,
): Promise<TxProtocolEventsResponse> {
  return requestData(
    apiClient.GET('/v1/tx/{tx_hash}/events', {
      params: {
        path: {
          tx_hash: txHash,
        },
      },
      signal,
    }),
    'GET /v1/tx/{tx_hash}/events',
  );
}

export function getTokens(
  filters?: {
    chain_id?: number;
    symbol?: string;
    limit?: number;
  },
  signal?: AbortSignal,
): Promise<TokensResponse> {
  return requestData(
    apiClient.GET('/v1/tokens', {
      params: { query: filters },
      signal,
    }),
    'GET /v1/tokens',
  );
}

export function getToken(
  tokenId: number,
  signal?: AbortSignal,
): Promise<Token> {
  return requestData(
    apiClient.GET('/v1/tokens/{token_id}', {
      params: {
        path: {
          token_id: tokenId,
        },
      },
      signal,
    }),
    'GET /v1/tokens/{token_id}',
  );
}

export function getTokenPrice(
  tokenId: number,
  signal?: AbortSignal,
): Promise<TokenPrice> {
  return requestData(
    apiClient.GET('/v1/tokens/{token_id}/price', {
      params: {
        path: {
          token_id: tokenId,
        },
      },
      signal,
    }),
    'GET /v1/tokens/{token_id}/price',
  );
}

export async function getPrimeDebtSnapshots(
  primeId: string,
  limit?: number,
  signal?: AbortSignal,
): Promise<PrimeDebtSnapshot[]> {
  const query =
    typeof limit === 'number'
      ? ({
          limit,
        } as paths['/v1/primes/{prime_id}/debt']['get']['parameters']['query'])
      : undefined;

  return requestData(
    apiClient.GET('/v1/primes/{prime_id}/debt', {
      params: {
        path: {
          prime_id: primeId,
        },
        query,
      },
      signal,
    }),
    'GET /v1/primes/{prime_id}/debt',
  );
}

export async function getLatestPrimeDebtSnapshot(
  primeId: string,
  signal?: AbortSignal,
): Promise<PrimeDebtSnapshot | null> {
  const snapshots = await getPrimeDebtSnapshots(primeId, 1, signal);
  return snapshots[0] ?? null;
}
