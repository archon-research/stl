import type { SortingState } from '@tanstack/react-table';

export function normalizeSearchString(
  value: string | null | undefined,
): string {
  if (!value) return '';
  return value
    .toLowerCase()
    .trim()
    .replace(/[^\w\s.]/g, '');
}

export function matchesSearchQuery(
  searchString: string | null | undefined,
  query: string | null | undefined,
): boolean {
  if (!query) return true;
  return normalizeSearchString(searchString).includes(
    normalizeSearchString(query),
  );
}

export function buildRowSearchString(
  fields: (string | number | null | undefined)[],
): string {
  return normalizeSearchString(
    fields
      .filter((f) => f !== null && f !== undefined)
      .map(String)
      .join(' '),
  );
}

export function validateSortingState(sorting: unknown): SortingState {
  if (
    Array.isArray(sorting) &&
    sorting.every(
      (item) =>
        typeof item === 'object' &&
        item !== null &&
        'id' in item &&
        'desc' in item &&
        typeof (item as Record<string, unknown>).id === 'string' &&
        typeof (item as Record<string, unknown>).desc === 'boolean',
    )
  ) {
    return sorting as SortingState;
  }
  return [];
}

export function serializeSorting(sorting: SortingState): string {
  if (!sorting || sorting.length === 0) return '';
  return sorting.map((s) => `${s.id}:${s.desc ? 'desc' : 'asc'}`).join(',');
}

export function deserializeSorting(
  query: string | null | undefined,
): SortingState {
  if (!query || query.trim() === '') return [];
  try {
    return query
      .split(',')
      .map((part) => {
        const [id, order] = part.split(':');
        if (!id) return null;
        return { id: id.trim(), desc: order?.trim() === 'desc' };
      })
      .filter((item): item is { id: string; desc: boolean } => item !== null);
  } catch {
    return [];
  }
}

export const ColumnIds = {
  SYMBOL: 'symbol',
  AMOUNT: 'amount',
  PRICE_USD: 'price_usd',
  AMOUNT_USD: 'amount_usd',
  BACKING_PCT: 'backing_pct',
  LIQUIDATION_THRESHOLD: 'liquidation_threshold',
  LIQUIDATION_BONUS: 'liquidation_bonus',
  BALANCE: 'balance',
  UNDERLYING_SYMBOL: 'underlying_symbol',
  CHAIN_ID: 'chain_id',
  PROTOCOL_NAME: 'protocol_name',
} as const;
