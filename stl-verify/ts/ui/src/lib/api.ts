import { createApiClient } from '@archon-research/http-client-react';

import type { paths } from '../generated/openapi-types';
import type {
  AllocationActivityResponse,
  AllocationsResponse,
  BadDebt,
  DataSourcesResponse,
  PrimesResponse,
  RiskBreakdown,
} from '../types/allocation';
import type {
  LocalChainRow,
  LocalProtocolRow,
  StarRiskCapitalResponse,
  StarRiskCapitalRow,
} from '../types/local-data';
import { logging } from './logging';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';
const STAR_RISK_CAPITAL_URL =
  'https://info-sky.blockanalitica.com/star-monitoring/risk-capital/primes/';
const apiClient = createApiClient<paths>(API_BASE_URL);

type BadDebtQuery =
  paths['/v1/risk/{receipt_token_id}/bad-debt']['get']['parameters']['query'];

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

export function getBadDebt(
  receiptTokenId: number,
  gapPct: BadDebtQuery['gap_pct'],
  signal?: AbortSignal,
): Promise<BadDebt> {
  return requestData(
    apiClient.GET('/v1/risk/{receipt_token_id}/bad-debt', {
      params: {
        path: { receipt_token_id: receiptTokenId },
        query: { gap_pct: gapPct },
      },
      signal,
    }),
    'GET /v1/risk/{receipt_token_id}/bad-debt',
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

export async function getStarRiskCapitalRequirements(
  signal?: AbortSignal,
): Promise<StarRiskCapitalRow[]> {
  try {
    // Create timeout signal (10s) that merges with provided signal
    const timeoutController = new AbortController();
    const timeoutId = setTimeout(() => timeoutController.abort(), 10000);

    const combinedSignal = signal
      ? AbortSignal.any([signal, timeoutController.signal])
      : timeoutController.signal;

    const response = await fetch(STAR_RISK_CAPITAL_URL, {
      signal: combinedSignal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      const responseText = await response.text().catch(() => '<no body>');

      logging.error('Star risk capital API request failed', {
        url: STAR_RISK_CAPITAL_URL,
        status: response.status,
        statusText: response.statusText,
        responseBody: responseText.slice(0, 500), // Limit log size
      });

      throw new Error(
        `Failed to fetch risk capital data (${response.status}): ${response.statusText}`,
      );
    }

    let payload: StarRiskCapitalResponse;
    try {
      payload = (await response.json()) as StarRiskCapitalResponse;
    } catch (jsonError) {
      logging.error('Failed to parse Star risk capital response as JSON', {
        error: jsonError,
        url: STAR_RISK_CAPITAL_URL,
      });
      throw new Error(
        'Received invalid JSON response from risk capital service',
        {
          cause: jsonError,
        },
      );
    }

    if (!payload.data?.results) {
      logging.warn(
        'Star risk capital response missing expected data structure',
        {
          hasData: !!payload.data,
          hasResults: !!payload.data?.results,
        },
      );
      return [];
    }

    return payload.data.results;
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      throw error; // Let abort errors propagate
    }

    // Network errors, timeouts, etc.
    logging.error('Network error fetching Star risk capital', {
      error,
      url: STAR_RISK_CAPITAL_URL,
    });
    throw error;
  }
}

export function getDataSources(
  signal?: AbortSignal,
): Promise<DataSourcesResponse> {
  return requestData(
    apiClient.GET('/v1/data-sources', { signal }),
    'GET /v1/data-sources',
  );
}
