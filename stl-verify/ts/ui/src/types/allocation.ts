import type { components, paths } from '../generated/openapi-types';

export type Prime = components['schemas']['PrimeResponse'];
export type Allocation = components['schemas']['AllocationResponse'];
export type AllocationActivity = components['schemas']['AllocationActivityResponse'];
export type CapitalMetrics = components['schemas']['CapitalMetricsResponse'];
export type DataSources = components['schemas']['DataSourcesResponse'];
export type DataSource = components['schemas']['DataSourceResponse'];
export type AllocationCategory = components['schemas']['AllocationCategory'];

export type RiskBreakdown = components['schemas']['RiskBreakdownResponse'];
export type BadDebt = components['schemas']['BadDebtResponse'];

export type PrimesResponse = NonNullable<
  paths['/v1/primes']['get']['responses']['200']['content']['application/json']
>;

export type AllocationsResponse = NonNullable<
  paths['/v1/primes/{prime_id}/allocations']['get']['responses']['200']['content']['application/json']
>;

export type AllocationActivityResponse = NonNullable<
  paths['/v1/allocations/activity']['get']['responses']['200']['content']['application/json']
>;

export type CapitalMetricsResponse = NonNullable<
  paths['/v1/primes/{prime_id}/capital-metrics']['get']['responses']['200']['content']['application/json']
>;

export type DataSourcesResponse = NonNullable<
  paths['/v1/data-sources']['get']['responses']['200']['content']['application/json']
>;
