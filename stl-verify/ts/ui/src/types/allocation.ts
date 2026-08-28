import type { components, paths } from '../generated/openapi-types';

export type Prime = components['schemas']['PrimeResponse'];
export type Allocation = components['schemas']['AllocationResponse'];
export type AllocationActivity =
  components['schemas']['AllocationActivityResponse'];
export type ProtocolEvent = components['schemas']['ProtocolEventResponse'];
export type PrimeRiskCapital =
  components['schemas']['PrimeRiskCapitalResponse'];
export type AllocationRiskCapital =
  components['schemas']['AllocationRiskCapitalResponse'];
export type DataSource = components['schemas']['DataSourceResponse'];
/** Which provenance a response was answered from -- not a `/v1/data-sources` row. */
export type Provenance = components['schemas']['Provenance'];
export type AllocationCategory = components['schemas']['AllocationCategory'];
export type PrimeDebtSnapshot =
  components['schemas']['PrimeDebtSnapshotResponse'];

export type RiskBreakdown = components['schemas']['RiskBreakdownResponse'];
export type RrcResult = components['schemas']['RrcResult'];

// Full envelope returned by the endpoint: { mode, window, data }.
export type AllocationActivityEnvelope = NonNullable<
  paths['/v1/allocations/activity']['get']['responses']['200']['content']['application/json']
>;

export type AllocationActivityBucket =
  components['schemas']['AllocationActivityBucketResponse'];

// The activity feed consumes the raw rows; the API client unwraps `data`.
export type AllocationActivityResponse = AllocationActivity[];

export type DataSourcesResponse = NonNullable<
  paths['/v1/data-sources']['get']['responses']['200']['content']['application/json']
>;

// Consumers use the raw rows; the API client unwraps `data`.
export type ProtocolEventsResponse = ProtocolEvent[];

export type TxProtocolEventsResponse = NonNullable<
  paths['/v1/tx/{tx_hash}/events']['get']['responses']['200']['content']['application/json']
>;

export type PrimeDebtEnvelope = components['schemas']['PrimeDebtEnvelope'];

export type PrimeDebtBucket = components['schemas']['PrimeDebtBucketResponse'];

export type TimeSeriesResolution =
  components['schemas']['TimeSeriesResolution'];

export type TotalCapitalEnvelope =
  components['schemas']['TotalCapitalEnvelope'];

export type TotalCapitalBucket =
  components['schemas']['TotalCapitalBucketResponse'];

export type ExposureEnvelope = components['schemas']['ExposureEnvelope'];

export type ExposureBucket = components['schemas']['ExposureBucketResponse'];

export type TokensResponse = NonNullable<
  paths['/v1/tokens']['get']['responses']['200']['content']['application/json']
>;
