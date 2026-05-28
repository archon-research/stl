import {
  buildRowSearchString,
  type CellContext,
  type ColumnDef,
  DataTable,
  LoadingIndicator,
  matchesSearchQuery,
  SkeletonStack,
  useDataTable,
} from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';

import { getRiskBreakdown, getToken, getTokenPrice } from '../../../lib/api';
import {
  formatDateTime,
  formatDurationFromSeconds,
  formatFreshnessLabel,
  formatMultiplier,
  formatPercentValue,
  formatRatioPercent,
  formatUsdValue,
  parseNumericValue,
} from '../../../lib/dashboard';
import { isAbortError, toErrorMessage } from '../../../lib/errors';
import { logging } from '../../../lib/logging';
import type {
  Allocation,
  RiskBreakdown,
  Token,
  TokenPrice,
} from '../../../types/allocation';
import { ChainLogo, SummaryMetric } from '../../shared';
import { MethodologyPanel } from '../../shared/MethodologyPanel';
import { TabErrorPanel, TabSelectionPrompt } from './TabStatePanels';

type RiskBreakdownTabProps = {
  isEnabled: boolean;
  searchQuery?: string;
  selectedReceiptToken: Allocation | null;
};

const tableHeaderTypographyClassName = css({
  '& thead th': {
    fontSize: 'sm',
    fontWeight: 'semibold',
    lineHeight: 'shorter',
    letterSpacing: '0.02em',
    textTransform: 'uppercase',
    color: 'text.default',
  },
  '& thead th button': {
    fontSize: 'sm',
    fontWeight: 'semibold',
    lineHeight: 'shorter',
    letterSpacing: '0.02em',
    textTransform: 'uppercase',
    color: 'text.default',
  },
});

type RiskItem = RiskBreakdown['items'][number];

function RiskSymbolCell({
  chainId,
  symbol,
}: {
  chainId: number;
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

function createRiskColumns(chainId: number): ColumnDef<RiskItem>[] {
  return [
    {
      id: 'symbol',
      header: 'Symbol',
      accessorKey: 'symbol',
      cell: (info: CellContext<RiskItem, unknown>) => (
        <RiskSymbolCell chainId={chainId} symbol={info.getValue() as string} />
      ),
    },
    {
      id: 'amount',
      header: 'Amount',
      accessorKey: 'amount',
      cell: (info: CellContext<RiskItem, unknown>) => {
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
      cell: (info: CellContext<RiskItem, unknown>) =>
        formatUsdValue(info.getValue() as string | number | null | undefined),
    },
    {
      id: 'amount_usd',
      header: 'Amount USD',
      accessorKey: 'amount_usd',
      cell: (info: CellContext<RiskItem, unknown>) =>
        formatUsdValue(info.getValue() as string | number | null | undefined),
    },
    {
      id: 'backing_pct',
      header: 'Backing %',
      accessorKey: 'backing_pct',
      cell: (info: CellContext<RiskItem, unknown>) =>
        formatPercentValue(
          info.getValue() as string | number | null | undefined,
        ),
    },
    {
      id: 'lt',
      header: 'Liquidation Threshold',
      accessorKey: 'liquidation_threshold',
      cell: (info: CellContext<RiskItem, unknown>) =>
        formatRatioPercent(
          info.getValue() as string | number | null | undefined,
        ),
    },
    {
      id: 'bonus',
      header: 'Liquidation Bonus',
      accessorKey: 'liquidation_bonus',
      cell: (info: CellContext<RiskItem, unknown>) =>
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
  chainId: number;
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
        getRowKey={(item) => String(item.token_id)}
        skeletonConfig={{ rows: 5, columns: 7, firstColumnTall: false }}
        minWidth="76rem"
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
}: RiskBreakdownTabProps) {
  const [breakdown, setBreakdown] = useState<RiskBreakdown | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isMethodologyOpen, setIsMethodologyOpen] = useState(false);
  const [tokenCatalog, setTokenCatalog] = useState<Token | null>(null);
  const [tokenPrice, setTokenPrice] = useState<TokenPrice | null>(null);
  const [isTokenMetaLoading, setIsTokenMetaLoading] = useState(false);

  const receiptTokenId = selectedReceiptToken?.receipt_token_id ?? null;

  useEffect(() => {
    const receiptTokenAddress = selectedReceiptToken?.receipt_token_address;
    if (
      !isEnabled ||
      !selectedReceiptToken ||
      receiptTokenId === null ||
      !receiptTokenAddress
    ) {
      setBreakdown(null);
      setErrorMessage(null);
      setIsLoading(false);
      return;
    }

    const controller = new AbortController();

    setIsLoading(true);
    setErrorMessage(null);
    setBreakdown(null);

    void getRiskBreakdown(
      selectedReceiptToken.chain_id,
      receiptTokenAddress,
      controller.signal,
    )
      .then((response) => {
        if (controller.signal.aborted) {
          return;
        }
        setBreakdown(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load risk breakdown', {
          error,
          chainId: selectedReceiptToken.chain_id,
          receiptTokenAddress,
        });
        setBreakdown(null);
        setErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      });

    return () => controller.abort();
  }, [isEnabled, receiptTokenId, selectedReceiptToken]);

  useEffect(() => {
    if (!isEnabled || !selectedReceiptToken) {
      setTokenCatalog(null);
      setTokenPrice(null);
      setIsTokenMetaLoading(false);
      return;
    }

    const chainId = selectedReceiptToken.chain_id;
    const underlyingAddress = selectedReceiptToken.underlying_token_address;
    if (!underlyingAddress) {
      setTokenCatalog(null);
      setTokenPrice(null);
      setIsTokenMetaLoading(false);
      return;
    }

    const controller = new AbortController();
    setIsTokenMetaLoading(true);

    void Promise.allSettled([
      getToken(chainId, underlyingAddress, controller.signal),
      getTokenPrice(chainId, underlyingAddress, controller.signal),
    ])
      .then(([tokenResult, priceResult]) => {
        if (controller.signal.aborted) {
          return;
        }

        if (tokenResult.status === 'fulfilled') {
          setTokenCatalog(tokenResult.value);
        } else {
          setTokenCatalog(null);
          logging.warn('Token catalog metadata unavailable for risk summary', {
            error: tokenResult.reason,
            chainId,
            underlyingAddress,
          });
        }

        if (priceResult.status === 'fulfilled') {
          setTokenPrice(priceResult.value);
        } else {
          setTokenPrice(null);
          logging.warn('Token price metadata unavailable for risk summary', {
            error: priceResult.reason,
            chainId,
            underlyingAddress,
          });
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsTokenMetaLoading(false);
        }
      });

    return () => controller.abort();
  }, [isEnabled, selectedReceiptToken]);

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
      }

      if (liquidationBonus !== null) {
        weightedBonus += liquidationBonus * amountUsd;
      }
    }

    return {
      assetCount: breakdown.items.length,
      largestItem,
      weightedBonus: totalUsd > 0 ? weightedBonus / totalUsd : null,
      weightedThreshold: totalUsd > 0 ? weightedThreshold / totalUsd : null,
    };
  }, [breakdown, totalUsd]);

  if (!selectedReceiptToken) {
    return (
      <TabSelectionPrompt message="Pick a receipt token to inspect its collateral backing." />
    );
  }

  if (receiptTokenId === null) {
    return (
      <div
        className={css({
          borderRadius: 'md',
          borderStyle: 'solid',
          borderWidth: '1px',
          borderColor: 'border.subtle',
          bg: 'surface.subtle',
          p: '4',
        })}
      >
        <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
          Direct asset holdings have no collateral backing to break down.
        </p>
      </div>
    );
  }

  return (
    <div className={css({ display: 'grid', gap: '4' })}>
      {isLoading ? <LoadingIndicator message="Loading risk breakdown" /> : null}

      {errorMessage ? (
        <TabErrorPanel
          title="Unable to load the risk breakdown."
          message={errorMessage}
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
              isTokenMetaLoading
                ? 'Fetching token metadata'
                : tokenCatalog
                  ? `${tokenCatalog.address} · ${tokenCatalog.decimals ?? 'Unknown'} decimals`
                  : 'Token metadata unavailable'
            }
          />
          <SummaryMetric
            label="Current price"
            value={
              isTokenMetaLoading
                ? 'Loading...'
                : tokenPrice
                  ? formatUsdValue(tokenPrice.price_usd)
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
        <div
          className={css({
            borderRadius: 'md',
            borderStyle: 'solid',
            borderWidth: '1px',
            borderColor: 'border.subtle',
            bg: 'surface.subtle',
            p: '4',
          })}
        >
          <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
            This receipt token returned no collateral items for the risk
            breakdown response.
          </p>
        </div>
      ) : null}

      {!errorMessage && (isLoading || breakdown) ? (
        <RiskTable
          chainId={selectedReceiptToken.chain_id}
          items={breakdown?.items ?? []}
          isLoading={isLoading}
          searchQuery={searchQuery}
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
