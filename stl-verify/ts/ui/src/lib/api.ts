import { createApiClient } from '@archon-research/http-client-react';

import {
  localChainRows,
  localCostRows,
  localProtocolRows,
} from '../generated/local-metadata';
import type { paths } from '../generated/openapi-types';
import type {
  AllocationsResponse,
  BadDebt,
  ReceiptTokensResponse,
  RiskBreakdown,
  StarsResponse,
} from '../types/allocation';
import type {
  LocalChainRow,
  LocalCostRow,
  LocalProtocolRow,
} from '../types/local-data';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';
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
  } catch {
    return 'Unserializable error body.';
  }
}

async function requestData<TData, TError>(
  request: ApiResult<TData, TError>,
  label: string,
): Promise<TData> {
  const { data, error, response } = await request;

  if (!response.ok || data === undefined) {
    throw new Error(
      `${label} failed (${response.status}): ${toErrorBody(error)}`,
    );
  }

  return data;
}

export function getStars(signal?: AbortSignal): Promise<StarsResponse> {
  return requestData(apiClient.GET('/v1/stars', { signal }), 'GET /v1/stars');
}

export function getLocalChains(): Promise<LocalChainRow[]> {
  return Promise.resolve(localChainRows);
}

export function getLocalProtocols(): Promise<LocalProtocolRow[]> {
  return Promise.resolve(localProtocolRows);
}

export function getLocalCosts(): Promise<LocalCostRow[]> {
  return Promise.resolve(localCostRows);
}

export function getAllocations(
  starId: string,
  signal?: AbortSignal,
): Promise<AllocationsResponse> {
  return requestData(
    apiClient.GET('/v1/stars/{star_id}/allocations', {
      params: { path: { star_id: starId } },
      signal,
    }),
    'GET /v1/stars/{star_id}/allocations',
  );
}

export function getReceiptTokens(
  starId: string,
  signal?: AbortSignal,
): Promise<ReceiptTokensResponse> {
  return requestData(
    apiClient.GET('/v1/stars/{star_id}/receipt-tokens', {
      params: { path: { star_id: starId } },
      signal,
    }),
    'GET /v1/stars/{star_id}/receipt-tokens',
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
