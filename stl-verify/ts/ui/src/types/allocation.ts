import type { components, paths } from '../generated/openapi-types';

export type Star = components['schemas']['StarResponse'];
export type AllocationPosition =
  components['schemas']['AllocationPositionResponse'];
export type ReceiptTokenPosition =
  components['schemas']['ReceiptTokenPositionResponse'];
export type RiskBreakdown = components['schemas']['RiskBreakdownResponse'];
export type BadDebt = components['schemas']['BadDebtResponse'];

export type StarsResponse = NonNullable<
  paths['/v1/stars']['get']['responses']['200']['content']['application/json']
>;

export type AllocationsResponse = NonNullable<
  paths['/v1/stars/{star_id}/allocations']['get']['responses']['200']['content']['application/json']
>;

export type ReceiptTokensResponse = NonNullable<
  paths['/v1/stars/{star_id}/receipt-tokens']['get']['responses']['200']['content']['application/json']
>;
