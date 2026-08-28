import { createApiClient } from '@archon-research/http-client-react';

import type { components, paths } from '../generated/openapi-types';
import type {
  AllocationActivityEnvelope,
  AllocationActivityResponse,
  DataSourcesResponse,
  ProtocolEventsResponse,
  RiskBreakdown,
  Rrc,
  Token,
  TokenPrice,
  TokensResponse,
  TxProtocolEventsResponse,
} from '../types/allocation';
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

// Carries the HTTP status so callers can branch on it (e.g. fall back only on
// 404) instead of parsing it back out of the message.
export class ApiRequestError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'ApiRequestError';
    this.status = status;
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

    throw new ApiRequestError(errorMessage, response.status);
  }

  return data;
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

async function getAllocationActivityEnvelope(
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
