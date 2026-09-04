import { type ColumnDef } from '@archon-research/design-system';

import {
  formatPercentValue,
  formatRatioPercent,
  type ChainLabelLookup,
} from '../../shared/lib/dashboard';
import type { LocalProtocolRow } from '../../shared/types/local-data';
import {
  AllocationActivityCell,
  AllocationAssetCell,
  AllocationCategoryCell,
  AllocationExposureCell,
  AllocationUnderlyingCell,
} from './AllocationGridCells';
import type { AllocationGridRow } from './allocationGridRows';
import {
  AllocationRatioCell,
  AllocationRiskCapitalCell,
  derivedRiskTitle,
  riskProvenanceTitle,
} from './AllocationRiskCells';

export function createAllocationColumns(
  chainLabels: ChainLabelLookup,
  localProtocols: LocalProtocolRow[],
): ColumnDef<AllocationGridRow>[] {
  return [
    {
      id: 'symbol',
      header: 'Asset',
      accessorFn: (allocation) => allocation.symbol,
      cell: ({ row }) => (
        <AllocationAssetCell
          allocation={row.original}
          chainLabels={chainLabels}
          localProtocols={localProtocols}
        />
      ),
    },
    {
      id: 'underlying_symbol',
      header: 'Underlying',
      accessorFn: (allocation) => allocation.underlying_symbol,
      cell: ({ row }) => <AllocationUnderlyingCell allocation={row.original} />,
    },
    {
      // Named for what it renders: `amount_usd`, the position's USD exposure —
      // the same quantity Sky's monitor publishes as EXPOSURE. The token
      // quantity appears only as the fallback for an unpriced row.
      id: 'exposure',
      header: 'Exposure',
      // Sorts on what the cell shows. Sorting the token balance instead would
      // order 4,722 BTC below 869M spUSDS while the column displays $250M above
      // $869M. An unpriced row has no exposure to sort by, so it sorts last
      // rather than tying with a genuine zero.
      accessorFn: (allocation) => allocation.risk.exposureUsd ?? -1,
      cell: ({ row }) => <AllocationExposureCell row={row.original} />,
      // Bar reflects USD value so magnitudes compare across heterogeneous
      // tokens; the cell text keeps the token holding. NaN (not null) suppresses
      // the bar for unpriced rows: a null here would fall back to the column
      // accessor (token balance), mixing token units into the USD domain.
      meta: {
        magnitude: {
          scale: 'linear',
          getValue: (allocation) => allocation.risk.exposureUsd ?? NaN,
          getValueText: () => null,
        },
      },
    },
    {
      id: 'latest_activity_at',
      header: 'Latest Activity',
      accessorFn: (allocation) => {
        const latestActivityAt = allocation.latest_activity_at;
        return latestActivityAt ? new Date(latestActivityAt).getTime() : 0;
      },
      cell: ({ row }) => <AllocationActivityCell allocation={row.original} />,
    },
    {
      id: 'category',
      header: 'Category',
      accessorFn: (allocation) => allocation.category,
      cell: ({ row }) => <AllocationCategoryCell allocation={row.original} />,
    },
    {
      id: 'risk_capital',
      // Named as Sky names it, since the two are compared side by side.
      header: 'RRC',
      // A row without a figure — chain-mismatched or no model — sorts below
      // genuine zeroes (-1) rather than tying with them.
      accessorFn: (allocation) =>
        allocation.risk.chainMismatch
          ? -1
          : (allocation.risk.riskCapitalUsd ?? -1),
      cell: ({ row }) => <AllocationRiskCapitalCell risk={row.original.risk} />,
      // No bar for n/a or chain-mismatched rows: NaN suppresses it (see
      // Balance for why not null).
      meta: {
        magnitude: {
          scale: 'linear',
          getValue: (allocation) =>
            allocation.risk.chainMismatch
              ? NaN
              : (allocation.risk.riskCapitalUsd ?? NaN),
          getValueText: () => null,
        },
        // Single-value USD cell, so the column can take mono + tabular figures
        // wholesale.
        mono: true,
        align: 'right',
      },
    },
    {
      id: 'crr',
      header: 'CRR',
      // A row with no ratio sorts below a genuine 0% rather than tying with it.
      accessorFn: (allocation) =>
        allocation.risk.chainMismatch ? -1 : (allocation.risk.crrPct ?? -1),
      cell: ({ row }) => (
        <AllocationRatioCell
          value={
            row.original.risk.chainMismatch ? null : row.original.risk.crrPct
          }
          format={formatPercentValue}
          state={row.original.risk.state}
          title={riskProvenanceTitle(row.original.risk)}
        />
      ),
      meta: {
        magnitude: {
          scale: 'linear',
          // Pinned to the ratio's own scale: a column-relative domain would
          // render 40/45/50% as empty→half→full, hiding the absolute level.
          domain: { min: 0, max: 100 },
          getValue: (allocation) =>
            allocation.risk.chainMismatch
              ? NaN
              : (allocation.risk.crrPct ?? NaN),
          getValueText: () => null,
        },
        mono: true,
        align: 'right',
      },
    },
    {
      id: 'rrc_share',
      header: 'RRC share',
      accessorFn: (allocation) =>
        allocation.risk.chainMismatch ? -1 : (allocation.risk.sharePct ?? -1),
      cell: ({ row }) => (
        <AllocationRatioCell
          value={
            row.original.risk.chainMismatch ? null : row.original.risk.sharePct
          }
          format={formatRatioPercent}
          state={row.original.risk.state}
          title={derivedRiskTitle(row.original.risk)}
        />
      ),
      meta: {
        magnitude: {
          scale: 'linear',
          getValue: (allocation) =>
            allocation.risk.chainMismatch
              ? NaN
              : (allocation.risk.sharePct ?? NaN),
          getValueText: () => null,
        },
        mono: true,
        align: 'right',
      },
    },
  ];
}
