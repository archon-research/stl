import { SkeletonStack } from '@archon-research/design-system';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import {
  formatDateTime,
  formatFreshnessLabel,
} from '../../shared/lib/dashboard';
import type { Prime } from '../../shared/types/allocation';
import { ProtocolLogo, TokenAddress } from '../../shared/ui';

type AllocationGridHeaderProps = {
  selectedPrime: Prime | null;
  showTopMetricsSkeleton: boolean;
  summary: { latestActivityAt: string | null } | null;
  isPrimeDebtLoading: boolean;
  primeDebtErrorMessage: string | null;
  debtTimestampLabel: string;
  debtObservedAt: string | null | undefined;
};

export function AllocationGridHeader({
  selectedPrime,
  showTopMetricsSkeleton,
  summary,
  isPrimeDebtLoading,
  primeDebtErrorMessage,
  debtTimestampLabel,
  debtObservedAt,
}: AllocationGridHeaderProps) {
  return (
    <div
      className={flex({
        align: 'flex-start',
        justify: 'space-between',
        gap: { base: '3', md: '4' },
        wrap: 'wrap',
      })}
    >
      <div
        className={css({
          display: 'grid',
          gap: '1',
          minWidth: { base: '0', md: '72' },
          flexGrow: '1',
          flexShrink: '1',
          flexBasis: '80',
        })}
      >
        <div className={flex({ align: 'center', gap: '2.5' })}>
          {selectedPrime ? (
            <>
              <ProtocolLogo protocolName={selectedPrime.name} size="8" />
              <h1
                className={css({
                  m: '0',
                  fontSize: { base: '3xl', md: '4xl' },
                  lineHeight: 'tight',
                  color: 'text.strong',
                })}
              >
                {selectedPrime.name}
              </h1>
            </>
          ) : (
            // Never "Select a prime": the page resolves one itself, so the
            // only time this is empty is before that has happened, and
            // naming an action the reader does not have to take reads as a
            // page that has given up.
            <SkeletonStack
              count={1}
              itemHeight={40}
              style={{ width: '12rem' }}
            />
          )}
        </div>
        {/* The label ships with the address, never on its own: this is the
            one place the prime's wallet address is named, and an unlabelled
            hex string here was read as a balance. */}
        {selectedPrime ? (
          <div
            className={flex({
              align: 'center',
              gap: '1.5',
              wrap: 'wrap',
              rowGap: '0',
            })}
          >
            <span
              className={css({
                fontSize: 'xs',
                color: 'text.muted',
                whiteSpace: 'nowrap',
              })}
            >
              Raw wallet address:
            </span>
            <TokenAddress address={selectedPrime.id} />
          </div>
        ) : null}
      </div>
      {!showTopMetricsSkeleton ? (
        <div
          className={css({
            display: 'flex',
            flexWrap: 'wrap',
            gap: { base: '2.5', md: '4' },
            justifyContent: { base: 'flex-start', md: 'flex-end' },
            textAlign: { base: 'left', md: 'right' },
            flexGrow: '1',
            flexShrink: '1',
            // 22rem falls between the 20rem and 24rem steps.
            flexBasis: '[22rem]',
          })}
        >
          {summary ? (
            <div
              className={css({
                display: 'flex',
                alignItems: 'center',
                gap: '1.5',
                flexWrap: 'wrap',
                justifyContent: 'flex-end',
              })}
            >
              <span
                className={css({
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                })}
              >
                Latest activity{' '}
                {summary.latestActivityAt
                  ? formatFreshnessLabel(summary.latestActivityAt)
                  : '—'}
              </span>
              <span
                className={css({
                  fontSize: 'xs',
                  // `short` is no token, so this shipped as an invalid
                  // `line-height: short` the browser drops; `snug` is 1.375.
                  lineHeight: 'snug',
                  color: 'text.muted',
                })}
              >
                {summary.latestActivityAt
                  ? formatDateTime(summary.latestActivityAt)
                  : 'No indexed activity'}
              </span>
            </div>
          ) : null}
          {selectedPrime ? (
            <div
              className={css({
                display: 'flex',
                alignItems: 'center',
                gap: '1.5',
                flexWrap: 'wrap',
                justifyContent: 'flex-end',
              })}
            >
              <span
                className={css({
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                })}
              >
                {debtTimestampLabel}{' '}
                {isPrimeDebtLoading
                  ? 'Loading...'
                  : primeDebtErrorMessage
                    ? 'Error'
                    : debtObservedAt
                      ? formatFreshnessLabel(debtObservedAt)
                      : '—'}
              </span>
              <span
                className={css({
                  fontSize: 'xs',
                  // `short` is no token, so this shipped as an invalid
                  // `line-height: short` the browser drops; `snug` is 1.375.
                  lineHeight: 'snug',
                  color: 'text.muted',
                })}
              >
                {isPrimeDebtLoading
                  ? 'Waiting for sync timestamp'
                  : primeDebtErrorMessage
                    ? primeDebtErrorMessage
                    : debtObservedAt
                      ? formatDateTime(debtObservedAt)
                      : 'No debt timestamp'}
              </span>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
