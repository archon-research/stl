import type { components, paths } from '../generated/openapi-types';

export type Prime = components['schemas']['PrimeResponse'];
export type Allocation = components['schemas']['AllocationResponse'];
export type RiskBreakdown = components['schemas']['RiskBreakdownResponse'];
export type BadDebt = components['schemas']['BadDebtResponse'];

export type PrimesResponse = NonNullable<
  paths['/v1/primes']['get']['responses']['200']['content']['application/json']
>;

export type AllocationsResponse = NonNullable<
  paths['/v1/primes/{prime_id}/allocations']['get']['responses']['200']['content']['application/json']
>;
