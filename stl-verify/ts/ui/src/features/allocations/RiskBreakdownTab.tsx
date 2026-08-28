import {
  buildRowSearchString,
  type CellContext,
  type ColumnDef,
  DataTable,
  ErrorState,
  matchesSearchQuery,
  SkeletonStack,
  useDataTable,
} from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';

import { css } from '#styled-system/css';

import {
  formatDateTime,
  formatDurationFromSeconds,
  formatFreshnessLabel,
  formatMultiplier,
  formatPercentValue,
  formatRatioPercent,
  formatUsdPrice,
  formatUsdValue,
  parseNumericValue,
} from '../../shared/lib/dashboard';
import { toQueryErrorMessage } from '../../shared/lib/errors';
import {
  DISABLED_ADDRESS,
  DISABLED_CHAIN_ID,
  riskBreakdownQuery,
  tokenPriceQuery,
  tokenQuery,
} from '../../shared/lib/queries';
import type {
  Allocation,
  Prime,
  RiskBreakdown,
} from '../../shared/types/allocation';
import {
  ChainLogo,
  SummaryMetric,
  tableHeaderTypographyClassName,
  TokenAddress,
  TruncatedLabel,
} from '../../shared/ui';
import {
  TabNotePanel,
  unindexedChainMessage,
} from '../../shared/ui/TabStatePanels';
import { MethodologyPanel } from './MethodologyPanel';

type RiskBreakdownTabProps = {
  isEnabled: boolean;
  searchQuery?: string;
  selectedReceiptToken: Allocation | null;
  selectedPrime: Prime | null;
};

type RiskItem = RiskBreakdown['items'][number];

// Stable while the breakdown is absent: `RiskTable` memoises on this identity.
const NO_RISK_ITEMS: RiskItem[] = [];

function RiskSymbolCell({
  chainId,
  symbol,
}: {
  chainId: number | null;
  symbol: string;
}) {
  return (
    <div
      className={css({
        display: 'inline-flex',
        alignItems: 'center',
        gap: '2',
      })}
    >
      <ChainLogo chainId={chainId} size="6" />
      <span>{symbol}</span>
    </div>
  );
}

// A long header otherwise sets the column's min-content width, so "Liquidation
// Threshold" reserved far more room than the percentages beneath it and pushed
// the last column out of view. Capping the label and letting it ellipsize lets
// the column size to its data instead; the full text stays in the DOM for
// assistive tech and comes back on hover through `TruncatedLabel`.
const truncatedHeaderClassName = css({
  display: 'block',
  maxWidth: '28',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
});

// Use this for any header long enough to outgrow the cap above; a short one
// would render identically, so it is only applied where it changes something.
function truncatingHeader(label: string) {
  return () => (
    <TruncatedLabel label={label} className={truncatedHeaderClassName} />
  );
}

function createRiskColumns(chainId: number | null): ColumnDef<RiskItem>[] {
  return [
    {
      id: 'symbol',
      header: 'Symbol',
      accessorKey: 'symbol',
      cell: (info: CellContext<RiskItem>) => (
        <RiskSymbolCell chainId={chainId} symbol={info.getValue() as string} />
      ),
    },
    {
      id: 'amount',
      header: 'Amount',
      accessorKey: 'amount',
      cell: (info: CellContext<RiskItem>) => {
        const value = info.getValue();
        return typeof value === 'string'
          ? parseFloat(value).toFixed(2)
          : (value as number).toFixed(2);
      },
    },
    {
      id: 'price_usd',
      header: 'Price USD',
      accessorKey: 'price_usd',
      cell: (info: CellContext<RiskItem>) =>
        formatUsdPrice(info.getValue() as string | number | null | undefined),
    },
    {
      id: 'amount_usd',
      header: 'Amount USD',
      accessorKey: 'amount_usd',
      cell: (info: CellContext<RiskItem>) =>
        formatUsdValue(info.getValue() as string | number | null | undefined),
      // The bar expresses each item's backing share of the position, so the USD
      // amount and its backing percentage live in one column instead of two.
      meta: {
        magnitude: {
          scale: 'linear',
          domain: { min: 0, max: 100 },
          getValue: (item) => parseNumericValue(item.backing_pct),
          getValueText: (value) => formatPercentValue(value),
        },
      },
    },
    {
      id: 'lt',
      header: truncatingHeader('Liquidation Threshold'),
      accessorKey: 'liquidation_threshold',
      cell: (info: CellContext<RiskItem>) =>
        formatRatioPercent(
          info.getValue() as string | number | null | undefined,
        ),
    },
    {
      id: 'bonus',
      header: truncatingHeader('Liquidation Bonus'),
      accessorKey: 'liquidation_bonus',
      cell: (info: CellContext<RiskItem>) =>
        formatMultiplier(info.getValue() as string | number | null | undefined),
    },
  ];
}

function RiskTable({
  chainId,
  items,
  isLoading,
  searchQuery,
}: {
  chainId: number | null;
  items: RiskItem[];
  isLoading: boolean;
  searchQuery: string;
}) {
  const filteredItems = useMemo(
    () =>
      items.filter((item) =>
        matchesSearchQuery(
          buildRowSearchString([
            item.symbol,
            item.amount,
            item.price_usd,
            item.amount_usd,
            item.backing_pct,
            item.liquidation_threshold,
            item.liquidation_bonus,
          ]),
          searchQuery,
        ),
      ),
    [items, searchQuery],
  );

  const columns = useMemo<ColumnDef<RiskItem>[]>(
    () => createRiskColumns(chainId),
    [chainId],
  );

  const table = useDataTable(filteredItems, columns, {
    enableSorting: true,
  });

  return (
    <div className={tableHeaderTypographyClassName}>
      <DataTable
        table={table}
        isLoading={isLoading}
        getRowKey={(item) => String(item.token_id ?? item.symbol)}
        skeletonConfig={{ rows: 5, firstColumnTall: false }}
        // Sized to what the six columns actually need once the long liquidation
        // headers ellipsize. The old 76rem exceeded the drawer this table lives
        // in, so the last column was always clipped no matter how wide the
        // drawer was dragged.
        minWidth="56rem"
        renderCell={(children) => (
          <div
            className={css({
              fontSize: 'sm',
              color: 'text.strong',
            })}
          >
            {children}
          </div>
        )}
      />
    </div>
  );
}

export function RiskBreakdownTab({
  isEnabled,
  searchQuery = '',
  selectedReceiptToken,
  selectedPrime,
}: RiskBreakdownTabProps) {
  const [isMethodologyOpen, setIsMethodologyOpen] = useState(false);

  const receiptTokenId = selectedReceiptToken?.receipt_token_id ?? null;
  const primeId = selectedPrime?.id ?? null;
  // A null chain is a position STL does not index, so there is no
  // (chain, receipt token) pair to ask about.
  const chainId = selectedReceiptToken?.chain_id ?? null;
  // Falsy rather than nullish, as the effect these replaced was: an empty
  // address would still build a request path that looks well-formed.
  const receiptTokenAddress =
    selectedReceiptToken?.receipt_token_address || null;
  const underlyingAddress =
    selectedReceiptToken?.underlying_token_address || null;
  // The breakdown scales to the given prime_id's pro-rata pool share on the
  // allocation's own chain_id, so it only resolves for the chain
  // selectedPrime actually holds a position on — the prime's primary proxy's
  // chain, today always mainnet. A non-mainnet allocation would find no
  // share data for that (chain_id, prime_id) pair.
  const isChainMismatch =
    selectedReceiptToken !== null &&
    selectedReceiptToken.chain_id !== null &&
    selectedPrime !== null &&
    selectedReceiptToken.chain_id !== selectedPrime.chain_id;

  const canLoadBreakdown =
    isEnabled &&
    chainId !== null &&
    receiptTokenId !== null &&
    receiptTokenAddress !== null &&
    !isChainMismatch;

  const breakdownResult = useQuery({
    ...riskBreakdownQuery(
      chainId ?? DISABLED_CHAIN_ID,
      receiptTokenAddress ?? DISABLED_ADDRESS,
      primeId,
    ),
    enabled: canLoadBreakdown,
  });

  const breakdown = breakdownResult.data ?? null;
  const isLoading = canLoadBreakdown && breakdownResult.isPending;
  const errorMessage = toQueryErrorMessage(breakdownResult.error);

  // Catalogue metadata for the *underlying*, which the breakdown does not carry.
  const canLoadTokenMeta =
    isEnabled && chainId !== null && underlyingAddress !== null;

  const tokenCatalogResult = useQuery({
    ...tokenQuery(
      chainId ?? DISABLED_CHAIN_ID,
      underlyingAddress ?? DISABLED_ADDRESS,
    ),
    enabled: canLoadTokenMeta,
  });
  const tokenPriceResult = useQuery({
    ...tokenPriceQuery(
      chainId ?? DISABLED_CHAIN_ID,
      underlyingAddress ?? DISABLED_ADDRESS,
    ),
    enabled: canLoadTokenMeta,
  });

  const tokenCatalog = tokenCatalogResult.data ?? null;
  const tokenPrice = tokenPriceResult.data ?? null;
  // One flag for both, because the summary reads them as a single block: a
  // price beside a still-loading symbol reads as a price for another token.
  // Either failing leaves its own value null, which the block already renders.
  const isTokenMetaLoading =
    canLoadTokenMeta &&
    (tokenCatalogResult.isPending || tokenPriceResult.isPending);

  const totalUsd = useMemo(() => {
    if (!breakdown) {
      return 0;
    }

    return breakdown.items.reduce(
      (sum, item) => sum + (parseNumericValue(item.amount_usd) ?? 0),
      0,
    );
  }, [breakdown]);

  const summary = useMemo(() => {
    if (!breakdown || breakdown.items.length === 0) {
      return null;
    }

    let weightedThreshold = 0;
    let weightedBonus = 0;
    // Track whether any item carried liquidation params; protocols without
    // per-asset params (e.g. Maple) leave these null so the summary shows "—"
    // rather than a misleading 0%/0x.
    let thresholdCount = 0;
    let bonusCount = 0;
    // USD weight of only the rows that carry each param. Dividing by totalUsd
    // instead would dilute the average toward 0 on a mixed dataset (rows without
    // the param would count in the denominator but not the numerator).
    let thresholdUsd = 0;
    let bonusUsd = 0;
    let largestItem = breakdown.items[0] ?? null;
    let largestItemUsd = largestItem
      ? (parseNumericValue(largestItem.amount_usd) ?? 0)
      : 0;

    for (const item of breakdown.items) {
      const amountUsd = parseNumericValue(item.amount_usd) ?? 0;
      const liquidationThreshold = parseNumericValue(
        item.liquidation_threshold,
      );
      const liquidationBonus = parseNumericValue(item.liquidation_bonus);

      if (amountUsd > largestItemUsd) {
        largestItem = item;
        largestItemUsd = amountUsd;
      }

      if (liquidationThreshold !== null) {
        weightedThreshold += liquidationThreshold * amountUsd;
        thresholdUsd += amountUsd;
        thresholdCount += 1;
      }

      if (liquidationBonus !== null) {
        weightedBonus += liquidationBonus * amountUsd;
        bonusUsd += amountUsd;
        bonusCount += 1;
      }
    }

    return {
      assetCount: breakdown.items.length,
      largestItem,
      weightedBonus:
        bonusUsd > 0 && bonusCount > 0 ? weightedBonus / bonusUsd : null,
      weightedThreshold:
        thresholdUsd > 0 && thresholdCount > 0
          ? weightedThreshold / thresholdUsd
          : null,
    };
  }, [breakdown]);

  if (!selectedReceiptToken) {
    return (
      <TabNotePanel message="Pick a receipt token to inspect its collateral backing." />
    );
  }

  if (selectedReceiptToken.chain_id === null) {
    return (
      <TabNotePanel
        message={unindexedChainMessage(
          selectedReceiptToken.network,
          'collateral backing',
        )}
      />
    );
  }

  if (receiptTokenId === null) {
    return (
      <TabNotePanel message="Direct asset holdings have no collateral backing to break down." />
    );
  }

  if (isChainMismatch) {
    return (
      <TabNotePanel message="Collateral backing is not yet available for non-mainnet allocations." />
    );
  }

  return (
    <div className={css({ display: 'grid', gap: '4' })}>
      {errorMessage ? (
        <ErrorState
          title="Unable to load the collateral backing."
          description={errorMessage}
          tone="critical"
          size="inline"
        />
      ) : null}

      {!errorMessage && summary ? (
        <div
          className={css({
            display: 'grid',
            gridTemplateColumns: {
              base: '1fr',
              md: 'repeat(4, minmax(0, 1fr))',
            },
            gap: '3',
          })}
        >
          <SummaryMetric
            label="Total backing"
            value={formatUsdValue(totalUsd)}
            detail={`${summary.assetCount} collateral assets`}
          />
          <SummaryMetric
            label="Largest exposure"
            value={summary.largestItem ? summary.largestItem.symbol : '—'}
            detail={
              summary.largestItem
                ? `${formatUsdValue(summary.largestItem.amount_usd)} · ${formatPercentValue(summary.largestItem.backing_pct)}`
                : undefined
            }
          />
          <SummaryMetric
            label="Weighted LT"
            value={formatRatioPercent(summary.weightedThreshold)}
          />
          <SummaryMetric
            label="Weighted bonus"
            value={formatMultiplier(summary.weightedBonus)}
          />
        </div>
      ) : null}

      {!errorMessage ? (
        <div
          className={css({
            display: 'grid',
            gridTemplateColumns: {
              base: '1fr',
              md: 'repeat(2, minmax(0, 1fr))',
            },
            gap: '3',
          })}
        >
          <SummaryMetric
            label="Token catalog"
            value={
              isTokenMetaLoading
                ? 'Loading...'
                : (tokenCatalog?.symbol ??
                  selectedReceiptToken.underlying_symbol)
            }
            detail={
              isTokenMetaLoading ? (
                'Fetching token metadata'
              ) : tokenCatalog ? (
                <span
                  className={css({
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '1.5',
                    flexWrap: 'wrap',
                  })}
                >
                  <TokenAddress
                    address={tokenCatalog.address}
                    chainId={selectedReceiptToken.chain_id}
                  />
                  <span>{tokenCatalog.decimals ?? 'Unknown'} decimals</span>
                </span>
              ) : (
                'Token metadata unavailable'
              )
            }
          />
          <SummaryMetric
            label="Current price"
            value={
              isTokenMetaLoading
                ? 'Loading...'
                : tokenPrice
                  ? formatUsdPrice(tokenPrice.price_usd)
                  : 'Unavailable'
            }
            detail={
              isTokenMetaLoading
                ? 'Fetching price metadata'
                : tokenPrice
                  ? tokenPrice.timestamp != null
                    ? [
                        `${tokenPrice.source_name} (${tokenPrice.source_type})`,
                        tokenPrice.staleness_seconds != null
                          ? `${formatDurationFromSeconds(tokenPrice.staleness_seconds)} stale`
                          : null,
                        formatFreshnessLabel(tokenPrice.timestamp),
                        formatDateTime(tokenPrice.timestamp),
                      ]
                        .filter(Boolean)
                        .join(' · ')
                    : 'Price data currently unavailable'
                  : 'Price metadata unavailable'
            }
          />
        </div>
      ) : null}

      {!errorMessage && isLoading && !summary ? (
        <SkeletonStack count={4} itemHeight={88} />
      ) : null}

      {!errorMessage &&
      !isLoading &&
      breakdown &&
      breakdown.items.length === 0 ? (
        <TabNotePanel message="This receipt token returned no collateral items for the collateral backing response." />
      ) : null}

      {!errorMessage && (isLoading || breakdown) ? (
        <RiskTable
          chainId={selectedReceiptToken.chain_id}
          items={breakdown?.items ?? NO_RISK_ITEMS}
          isLoading={isLoading}
          searchQuery={searchQuery}
        />
      ) : null}

      {!errorMessage &&
      selectedReceiptToken.protocol_name?.toLowerCase() === 'maple' ? (
        <TabNotePanel
          message={
            'Source: Maple Finance GraphQL API. Collateral amounts and USD values are attested by Maple/custodians and are not independently verified on-chain. Internal (AMM/strategy) loans are excluded; the breakdown reflects external-loan collateral plus available pool liquidity, so it is less than total supply.' +
            (selectedPrime
              ? ' Per-prime USD values are a pro-rata approximation (each pool asset scaled by the prime’s pool share) and will not match data.spark.fi, which uses a different (coverage-capped) attribution model. Backing % is a pool property and is identical for every prime.'
              : '')
          }
        />
      ) : null}

      {/* Data Sources & Methodology Footer */}
      <MethodologyPanel
        isOpen={isMethodologyOpen}
        onToggle={() => setIsMethodologyOpen(!isMethodologyOpen)}
        selectedChainId={selectedReceiptToken.chain_id}
        selectedTokenAddress={selectedReceiptToken.underlying_token_address}
        selectedTokenSymbol={selectedReceiptToken.underlying_symbol}
      />
    </div>
  );
}
