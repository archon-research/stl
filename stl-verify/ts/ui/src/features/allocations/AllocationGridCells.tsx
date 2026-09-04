import { Badge } from '@archon-research/design-system';

import { css, cx } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import {
  formatDateTime,
  formatFreshnessLabel,
  formatTokenAmount,
  formatUsdValue,
  getCategoryLabel,
  getChainLabel,
  getProtocolLabel,
  parseNumericValue,
  type ChainLabelLookup,
} from '../../shared/lib/dashboard';
import { useProvenanceView } from '../../shared/lib/provenance';
import type {
  Allocation,
  AllocationCategory,
} from '../../shared/types/allocation';
import type { LocalProtocolRow } from '../../shared/types/local-data';
import {
  ChainLogo,
  ProtocolLogo,
  TokenAddress,
  TokenLogo,
} from '../../shared/ui';
import { getActionColorClass, getActionIcon } from '../activity/action-styles';
import type { AllocationGridRow } from './allocationGridRows';

// Fill override for the `Badge` these chips render as: `Badge`'s `colorPalette`
// ships six status-flavoured hues, so it cannot give five strategy categories
// distinct fills, and its red would read as an alarm on a routine category. The
// `categorical.*` tokens encode grouping without status meaning, and their hue
// order matches `chart.series`, so a chip and its series line read as the same
// category. Everything else about the chip — radius, weight, size, padding — is
// the recipe's.
//
// One literal `css()` call per category, evaluated at module scope, so the cell
// picks a finished class name: see `features/activity/action-styles.tsx` for why Panda cannot
// extract a token path handed in as a variable.
const CATEGORY_CHIP_CLASS: Record<AllocationCategory | 'unknown', string> = {
  allocation: css({ bg: 'categorical.1.bg', color: 'categorical.1.fg' }),
  pol: css({ bg: 'categorical.2.bg', color: 'categorical.2.fg' }),
  psm3: css({ bg: 'categorical.3.bg', color: 'categorical.3.fg' }),
  asset: css({ bg: 'categorical.4.bg', color: 'categorical.4.fg' }),
  custody: css({ bg: 'categorical.5.bg', color: 'categorical.5.fg' }),
  // No override: `Badge`'s own subtle × neutral default is this fill.
  unknown: '',
};

function getCategoryChipClass(
  category: AllocationCategory | undefined,
): string {
  // `AllocationCategory` is a compile-time union over an unvalidated API response,
  // so a category the backend adds later arrives as an unlisted string. Keying on
  // own-property presence rather than `?? 'unknown'` means that renders the neutral
  // chip instead of an unstyled one -- matching how getCategoryLabel already
  // degrades.
  return category !== undefined && Object.hasOwn(CATEGORY_CHIP_CLASS, category)
    ? CATEGORY_CHIP_CLASS[category]
    : CATEGORY_CHIP_CLASS.unknown;
}

/**
 * The badge text for a row only one provenance reported, or `null`.
 *
 * Both provenances get a badge, not just Sky's: under `source=both` the API
 * carries a row whichever side reported it, so an STL-only row is as much a
 * single-sourced figure as a Sky-only one — and it can be a large one. The
 * merged mainnet spark response puts a $57M `spWETH` position in that bucket,
 * well above several rows that do carry the Sky-only mark.
 *
 * This marks most of the table rather than a handful, and that is the honest
 * reading: Sky's monitor covers mainnet spark far more than the other chains,
 * so across a vault's primes a corroborated row is the exception. Spark's vault
 * view badges 25 of 33 rows. A bare row means both sides reported the position,
 * which is the claim worth being able to trust on sight.
 */
function soleReporterLabel(
  source: Allocation['source'],
  shown: { showsIndexed: boolean; showsReference: boolean },
): string | null {
  if (source === 'reference') return shown.showsIndexed ? 'Legacy only' : null;
  if (source === 'indexed') return shown.showsReference ? 'Verify only' : null;
  return null;
}

export function AllocationAssetCell({
  allocation,
  localProtocols,
  chainLabels,
}: {
  allocation: Allocation;
  localProtocols: LocalProtocolRow[];
  chainLabels: ChainLabelLookup;
}) {
  // A badge marks a row against the other provenance's rows, so it says nothing
  // unless those are on screen too — which is only the merged view, since a
  // single-provenance response holds nothing to stand out from.
  const { showsIndexed: showsIndexedNow, showsReference: showsReferenceNow } =
    useProvenanceView();
  const soleReporter = soleReporterLabel(allocation.source, {
    showsIndexed: showsIndexedNow,
    showsReference: showsReferenceNow,
  });
  const chainLabel = getChainLabel(
    allocation.chain_id,
    chainLabels,
    allocation.network,
  );

  return (
    <div className={css({ display: 'grid', gap: '1', minWidth: '0' })}>
      <div className={flex({ align: 'center', gap: '1.5', wrap: 'wrap' })}>
        <p
          className={css({
            m: '0',
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
          })}
        >
          {allocation.symbol}
        </p>
        {/* Absent exactly when both provenances reported the row — see
            `soleReporterLabel` for why that, and not scarcity, is the rule. */}
        {soleReporter === null ? null : (
          <Badge size="sm" variant="subtle">
            {soleReporter}
          </Badge>
        )}
      </div>
      <div className={flex({ gap: '1.5', wrap: 'wrap' })}>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.muted',
            display: 'inline-flex',
            alignItems: 'center',
            gap: '1.5',
            whiteSpace: 'nowrap',
          })}
        >
          <ProtocolLogo
            protocolName={getProtocolLabel(
              allocation.protocol_name,
              localProtocols,
              allocation.chain_id,
            )}
            size="5"
          />
          {getProtocolLabel(
            allocation.protocol_name,
            localProtocols,
            allocation.chain_id,
          )}
        </span>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.muted',
            display: 'inline-flex',
            alignItems: 'center',
            gap: '1.5',
            whiteSpace: 'nowrap',
          })}
        >
          <ChainLogo
            chainId={allocation.chain_id}
            label={chainLabel}
            size="5"
          />
          {chainLabel}
        </span>
      </div>
    </div>
  );
}

export function AllocationUnderlyingCell({
  allocation,
}: {
  allocation: Allocation;
}) {
  // Legacy's balance-sheet rows name no loan token at all, so both the symbol
  // and the address are absent. Rendering the cell anyway drew an empty avatar
  // above a dash — two placeholders for one missing value, and the avatar read
  // as a token whose logo had failed to load. One dash, as every other column
  // does for an absent value.
  if (!allocation.underlying_symbol && !allocation.underlying_token_address) {
    return (
      <p className={css({ m: '0', fontSize: 'sm', color: 'text.muted' })}>—</p>
    );
  }

  return (
    <div
      className={css({
        display: 'flex',
        flexDirection: 'column',
        gap: '1',
      })}
    >
      <div className={flex({ align: 'center', gap: '2' })}>
        <TokenLogo
          address={allocation.underlying_token_address}
          chainId={allocation.chain_id}
          size="6"
          symbol={allocation.underlying_symbol}
        />
        <span
          className={css({
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            m: '0',
          })}
        >
          {allocation.underlying_symbol}
        </span>
      </div>
      <TokenAddress
        address={allocation.underlying_token_address}
        chainId={allocation.chain_id}
        compact
        style={{ fontSize: '0.8rem' }}
      />
    </div>
  );
}

export function AllocationExposureCell({ row }: { row: AllocationGridRow }) {
  const allocation = row;
  // The column shows Verify's value and falls back to Legacy's, so the title
  // names whichever side is not on display — including the case where Verify
  // has no figure at all, which a bare number would otherwise pass off as its
  // own valuation.
  const exposureUsd = row.risk.exposureUsd;
  const verifyUsd = parseNumericValue(allocation.amount_usd);
  const legacyUsd = parseNumericValue(allocation.reference_amount_usd);
  const valuationTitle =
    verifyUsd === null
      ? legacyUsd === null
        ? undefined
        : "Legacy's value; Verify prices none of this position"
      : legacyUsd === null
        ? undefined
        : `Verify's value; Legacy reports ${formatUsdValue(legacyUsd)}`;

  return (
    <div
      className={css({
        display: 'flex',
        flexDirection: 'column',
        gap: '1',
      })}
    >
      <div className={flex({ align: 'center', gap: '2' })}>
        <TokenLogo
          address={allocation.receipt_token_address}
          chainId={allocation.chain_id}
          protocolName={allocation.protocol_name}
          size="6"
          symbol={allocation.symbol}
        />
        <span
          className={css({
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            // Tabular figures so amounts align down the column. Not the
            // column's `meta.mono`: this cell is a composite (logo, value,
            // address) and mono would restyle all of it.
            fontVariantNumeric: 'tabular-nums',
            m: '0',
          })}
        >
          <span title={valuationTitle}>
            {exposureUsd !== null
              ? formatUsdValue(exposureUsd)
              : `${formatTokenAmount(allocation.balance)} ${allocation.symbol}`}
          </span>
        </span>
      </div>
      <TokenAddress
        address={allocation.receipt_token_address}
        chainId={allocation.chain_id}
        compact
        style={{ fontSize: '0.8rem' }}
      />
    </div>
  );
}

// Approximate the latest flow's USD value from the position's current implied
// price (amount_usd / balance) rather than a historical price: the activity
// row carries only a token-unit tx_amount, and this is a magnitude annotation,
// not an accounting figure. Falls back to the token amount when unpriced.
function formatActivityMagnitude(allocation: Allocation): string | null {
  const amount = parseNumericValue(allocation.latest_activity_amount);
  // Sweeps are internal reallocations with tx_amount 0; show the icon alone
  // rather than a misleading "$0.00".
  if (amount === null || amount === 0) {
    return null;
  }

  const action = allocation.latest_activity_action?.toLowerCase();
  const sign = action === 'in' ? '+' : action === 'out' ? '-' : '';

  const balance = parseNumericValue(allocation.balance);
  const amountUsd = parseNumericValue(allocation.amount_usd);
  if (amountUsd !== null && balance !== null && balance > 0) {
    return `${sign}${formatUsdValue(amount * (amountUsd / balance))}`;
  }

  return `${sign}${formatTokenAmount(amount)} ${allocation.symbol}`;
}

export function AllocationActivityCell({
  allocation,
}: {
  allocation: Allocation;
}) {
  if (!allocation.latest_activity_at) {
    return (
      <p
        className={css({
          m: '0',
          fontSize: 'sm',
          color: 'text.muted',
        })}
      >
        —
      </p>
    );
  }

  const actionColorClass = getActionColorClass(
    allocation.latest_activity_action,
  );
  const actionIcon = getActionIcon(allocation.latest_activity_action);
  const magnitude = formatActivityMagnitude(allocation);

  return (
    <div>
      <div className={flex({ align: 'center', gap: '1.5' })}>
        {actionIcon ? (
          <span
            className={cx(css({ display: 'inline-flex' }), actionColorClass)}
          >
            {actionIcon}
          </span>
        ) : null}
        <span
          className={css({
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
          })}
        >
          {formatFreshnessLabel(allocation.latest_activity_at)}
        </span>
        {magnitude ? (
          <span
            className={cx(
              css({
                fontSize: 'xs',
                fontWeight: 'medium',
                whiteSpace: 'nowrap',
              }),
              actionColorClass,
            )}
          >
            {magnitude}
          </span>
        ) : null}
      </div>
      <p
        className={css({
          m: '0',
          fontSize: 'xs',
          color: 'text.muted',
        })}
      >
        {formatDateTime(allocation.latest_activity_at)}
      </p>
    </div>
  );
}

export function AllocationCategoryCell({
  allocation,
}: {
  allocation: Allocation;
}) {
  const category = allocation.category;

  return (
    <Badge className={getCategoryChipClass(category)}>
      {getCategoryLabel(category)}
    </Badge>
  );
}
