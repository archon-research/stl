/**
 * Names for the generated schema types the fixtures are written against; see the
 * README for why they come from the ui workspace.
 */
import type { components, paths } from '@stl-verify/ui/openapi-types';

export type { paths };

type Schemas = components['schemas'];

export type Allocation = Schemas['AllocationResponse'];
export type AllocationActivity = Schemas['AllocationActivityResponse'];
export type AllocationActivityBucket =
  Schemas['AllocationActivityBucketResponse'];
export type AllocationActivityEnvelope = Schemas['AllocationActivityEnvelope'];
export type AllocationRiskCapital = Schemas['AllocationRiskCapitalResponse'];
export type CapitalMetrics = Schemas['CapitalMetricsResponse'];
export type Chain = Schemas['ChainResponse'];
export type DataSource = Schemas['DataSourceResponse'];
export type ExposureEnvelope = Schemas['ExposureEnvelope'];
export type Prime = Schemas['PrimeResponse'];
export type PrimeDebtBucket = Schemas['PrimeDebtBucketResponse'];
export type PrimeDebtEnvelope = Schemas['PrimeDebtEnvelope'];
export type PrimeDebtSnapshot = Schemas['PrimeDebtSnapshotResponse'];
export type PrimeRiskCapital = Schemas['PrimeRiskCapitalResponse'];
export type Protocol = Schemas['ProtocolResponse'];
export type ProtocolEvent = Schemas['ProtocolEventResponse'];
export type ProtocolEventsEnvelope = Schemas['ProtocolEventsEnvelope'];
export type RiskBreakdown = Schemas['RiskBreakdownResponse'];
export type RiskBreakdownItem = Schemas['RiskBreakdownItemResponse'];
export type RrcEnvelope = Schemas['RrcEnvelope'];
export type TimeSeriesResolution = Schemas['TimeSeriesResolution'];
export type TimeSeriesWindow = Schemas['TimeSeriesWindow'];
export type Token = Schemas['TokenResponse'];
export type TokenPrice = Schemas['TokenPriceResponse'];
export type TotalCapitalEnvelope = Schemas['TotalCapitalEnvelope'];
