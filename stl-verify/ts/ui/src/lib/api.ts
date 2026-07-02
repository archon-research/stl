import { createApiClient } from '@archon-research/http-client-react';

import type { components, paths } from '../generated/openapi-types';
import type {
  AllocationActivityEnvelope,
  AllocationActivityResponse,
  AllocationsResponse,
  CapitalMetricsListResponse,
  DataSourcesResponse,
  ExposureEnvelope,
  PrimeDebtEnvelope,
  PrimeRiskCapital,
  PrimeDebtSnapshot,
  PrimesResponse,
  ProtocolEventsResponse,
  RiskBreakdown,
  Rrc,
  Token,
  TokenPrice,
  TokensResponse,
  TotalCapitalEnvelope,
  TxProtocolEventsResponse,
} from '../types/allocation';
import type { LocalChainRow, LocalProtocolRow } from '../types/local-data';
import { isAbortError } from './errors';
import { logging } from './logging';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';
const apiClient = createApiClient<paths>(API_BASE_URL);

type TimeSeriesResolution = components['schemas']['TimeSeriesResolution'];

// Shared query shape for the bucketed time-series endpoints (allocation
// activity, prime debt, total capital).
type TimeSeriesFilters = {
  from_timestamp?: string;
  to_timestamp?: string;
  resolution?: TimeSeriesResolution;
  aggregate?: boolean;
  limit?: number;
};

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

export function getPrimeRiskCapital(
  primeId: string,
  signal?: AbortSignal,
): Promise<PrimeRiskCapital> {
  return requestData(
    apiClient.GET('/v1/primes/{prime_id}/risk-capital', {
      params: { path: { prime_id: primeId } },
      signal,
    }),
    'GET /v1/primes/{prime_id}/risk-capital',
  );
}

export async function getExposureEnvelope(
  primeId: string,
  filters?: {
    from_timestamp?: string;
    to_timestamp?: string;
    resolution?: TimeSeriesResolution;
    aggregate?: boolean;
    limit?: number;
  },
  signal?: AbortSignal,
): Promise<ExposureEnvelope> {
  const query = filters as
    | paths['/v1/primes/{prime_id}/exposure']['get']['parameters']['query']
    | undefined;

  const envelope = await requestData(
    apiClient.GET('/v1/primes/{prime_id}/exposure', {
      params: {
        path: {
          prime_id: primeId,
        },
        query,
      },
      signal,
    }),
    'GET /v1/primes/{prime_id}/exposure',
  );
  return envelope as ExposureEnvelope;
}

export function getRiskBreakdown(
  chainId: number,
  tokenAddress: string,
  primeId?: string | null,
  signal?: AbortSignal,
): Promise<RiskBreakdown> {
  return requestData(
    apiClient.GET('/v1/risk/{chain_id}/{token_address}/breakdown', {
      params: {
        path: { chain_id: chainId, token_address: tokenAddress },
        query: primeId ? { prime_id: primeId } : undefined,
      },
      signal,
    }),
    'GET /v1/risk/{chain_id}/{token_address}/breakdown',
  );
}

export function getRrc(
  chainId: number,
  tokenAddress: string,
  primeAddress: string,
  signal?: AbortSignal,
): Promise<Rrc> {
  return requestData(
    apiClient.GET('/v1/risk/rrc', {
      params: {
        query: {
          chain_id: chainId,
          prime_id: primeAddress,
          token_address: tokenAddress,
        },
      },
      signal,
    }),
    'GET /v1/risk/rrc',
  );
}

type AllocationActivityFilters = TimeSeriesFilters & {
  prime_id?: string;
  chain_id?: number;
  protocol_name?: string;
  action_type?: string;
  token_symbol?: string;
  tx_hash?: string;
};

export async function getAllocationActivity(
  filters?: AllocationActivityFilters,
  signal?: AbortSignal,
): Promise<AllocationActivityResponse> {
  const envelope = await getAllocationActivityEnvelope(filters, signal);
  // This helper returns raw rows; an aggregated envelope (aggregate=true) holds
  // bucket rows of an incompatible shape, so surface the misuse rather than
  // handing back mis-typed data.
  if (envelope.mode !== 'raw') {
    throw new Error(
      `GET /v1/allocations/activity returned "${envelope.mode}" for a raw activity request`,
    );
  }
  return (envelope.data ?? []) as AllocationActivityResponse;
}

export async function getAllocationActivityEnvelope(
  filters?: AllocationActivityFilters,
  signal?: AbortSignal,
): Promise<AllocationActivityEnvelope> {
  const envelope = await requestData(
    apiClient.GET('/v1/allocations/activity', {
      params: { query: filters },
      signal,
    }),
    'GET /v1/allocations/activity',
  );
  return envelope as AllocationActivityEnvelope;
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

export async function getProtocolEvents(
  filters?: {
    tx_hash?: string;
    protocol_name?: string;
    limit?: number;
  },
  signal?: AbortSignal,
): Promise<ProtocolEventsResponse> {
  const envelope = await requestData(
    apiClient.GET('/v1/protocol-events', {
      params: { query: filters },
      signal,
    }),
    'GET /v1/protocol-events',
  );
  return (envelope.data ?? []) as ProtocolEventsResponse;
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
  chainId: number,
  tokenAddress: string,
  signal?: AbortSignal,
): Promise<Token> {
  return requestData(
    apiClient.GET('/v1/tokens/{chain_id}/{token_address}', {
      params: {
        path: {
          chain_id: chainId,
          token_address: tokenAddress,
        },
      },
      signal,
    }),
    'GET /v1/tokens/{chain_id}/{token_address}',
  );
}

export function getTokenPrice(
  chainId: number,
  tokenAddress: string,
  signal?: AbortSignal,
): Promise<TokenPrice> {
  return requestData(
    apiClient.GET('/v1/tokens/{chain_id}/{token_address}/price', {
      params: {
        path: {
          chain_id: chainId,
          token_address: tokenAddress,
        },
      },
      signal,
    }),
    'GET /v1/tokens/{chain_id}/{token_address}/price',
  );
}

export async function getPrimeDebtSnapshots(
  primeId: string,
  filters?: TimeSeriesFilters,
  signal?: AbortSignal,
): Promise<PrimeDebtSnapshot[]> {
  const envelope = await getPrimeDebtEnvelope(primeId, filters, signal);
  // Raw snapshots only; an aggregated envelope holds bucket rows, so reject it
  // rather than returning mis-typed data.
  if (envelope.mode !== 'raw') {
    throw new Error(
      `GET /v1/primes/{prime_id}/debt returned "${envelope.mode}" for a snapshot request`,
    );
  }
  return (envelope.data ?? []) as PrimeDebtSnapshot[];
}

export async function getPrimeDebtEnvelope(
  primeId: string,
  filters?: TimeSeriesFilters,
  signal?: AbortSignal,
): Promise<PrimeDebtEnvelope> {
  const query = filters as
    | paths['/v1/primes/{prime_id}/debt']['get']['parameters']['query']
    | undefined;

  const envelope = await requestData(
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
  return envelope as PrimeDebtEnvelope;
}

export async function getTotalCapitalEnvelope(
  primeId: string,
  filters?: TimeSeriesFilters,
  signal?: AbortSignal,
): Promise<TotalCapitalEnvelope> {
  const query = filters as
    | paths['/v1/primes/{prime_id}/total-capital']['get']['parameters']['query']
    | undefined;

  const envelope = await requestData(
    apiClient.GET('/v1/primes/{prime_id}/total-capital', {
      params: {
        path: {
          prime_id: primeId,
        },
        query,
      },
      signal,
    }),
    'GET /v1/primes/{prime_id}/total-capital',
  );
  return envelope as TotalCapitalEnvelope;
}

export async function getLatestPrimeDebtSnapshot(
  primeId: string,
  signal?: AbortSignal,
): Promise<PrimeDebtSnapshot | null> {
  const snapshots = await getPrimeDebtSnapshots(
    primeId,
    {
      limit: 1,
    },
    signal,
  );
  return snapshots[0] ?? null;
}
