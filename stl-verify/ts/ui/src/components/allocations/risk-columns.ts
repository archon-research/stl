import type { ColumnDef } from '@tanstack/react-table';

import {
  formatUsdValue,
} from '../../lib/dashboard';
import type { RiskBreakdown } from '../../types/allocation';

type RiskItem = RiskBreakdown['items'][number];


/**
 * Risk breakdown table column definitions
 * Used by RiskBreakdownTab to render sortable risk breakdown items
 */
export const riskBreakdownColumns: ColumnDef<RiskItem>[] = [
  {
    id: 'symbol',
    header: 'Symbol',
    accessorKey: 'symbol',
    cell: (info) => info.getValue() as string,
  },
  {
    id: 'amount',
    header: 'Amount',
    accessorKey: 'amount',
    cell: (info) => {
      const value = info.getValue() as string | number;
      return typeof value === 'string' ? parseFloat(value).toFixed(2) : value;
    },
  },
  {
    id: 'price_usd',
    header: 'Price USD',
    accessorKey: 'price_usd',
    cell: (info) => formatUsdValue(info.getValue() as string | number),
  },
  {
    id: 'amount_usd',
    header: 'Amount USD',
    accessorKey: 'amount_usd',
    cell: (info) => formatUsdValue(info.getValue() as string | number),
  },
  {
    id: 'backing_pct',
    header: 'Backing %',
    accessorKey: 'backing_pct',
    cell: (info) => {
      const value = info.getValue() as string | number;
      return typeof value === 'string'
        ? `${(parseFloat(value) * 100).toFixed(2)}%`
        : `${(value * 100).toFixed(2)}%`;
    },
  },
  {
    id: 'lt',
    header: 'Liquidation Threshold',
    accessorKey: 'liquidation_threshold',
    cell: (info) => {
      const value = info.getValue() as string | number | null;
      if (value === null) return '—';
      return typeof value === 'string'
        ? `${(parseFloat(value) * 100).toFixed(2)}%`
        : `${(value * 100).toFixed(2)}%`;
    },
  },
  {
    id: 'bonus',
    header: 'Liquidation Bonus',
    accessorKey: 'liquidation_bonus',
    cell: (info) => {
      const value = info.getValue() as string | number | null;
      if (value === null) return '—';
      return typeof value === 'string'
        ? `${(parseFloat(value) * 100).toFixed(2)}%`
        : `${(value * 100).toFixed(2)}%`;
    },
  },
];
