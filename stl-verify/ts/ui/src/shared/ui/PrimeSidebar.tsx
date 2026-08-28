import {
  AsyncStateRenderer,
  EmptyState,
  ErrorState,
  SkeletonStack,
  Switch,
  ThemeToggle,
} from '@archon-research/design-system';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';
import { toggleSwitch } from '#styled-system/recipes';

import { ProtocolLogo } from '.';
import { truncateMiddle } from '../lib/dashboard';
import type { PrimeGroup } from '../lib/dashboard';

type PrimeSidebarProps = {
  primeGroups: PrimeGroup[];
  selectedPrimeId: string | null;
  isLoading: boolean;
  errorMessage: string | null;
  onSelectPrime: (primeKey: string) => void;
  showAllPrimes: boolean;
  canShowAllPrimes: boolean;
  onShowAllPrimesChange: (value: boolean) => void;
};

const switchStyles = toggleSwitch();

export function PrimeSidebar({
  primeGroups,
  selectedPrimeId,
  isLoading,
  errorMessage,
  onSelectPrime,
  showAllPrimes,
  canShowAllPrimes,
  onShowAllPrimesChange,
}: PrimeSidebarProps) {
  const primeButtonsDisabled = showAllPrimes && canShowAllPrimes;
  return (
    <div
      className={css({
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        maxWidth: '100%',
        height: '100%',
        minHeight: 0,
        overflow: 'hidden',
        boxSizing: 'border-box',
        bg: 'surface.default',
      })}
    >
      <div
        className={css({
          width: '100%',
          boxSizing: 'border-box',
          px: '5',
          py: '4',
          borderBottomWidth: '1px',
          borderBottomStyle: 'solid',
          borderBottomColor: 'border.subtle',
        })}
      >
        <div className={flex({ align: 'center', gap: '3.5' })}>
          <img
            src="/assets/archon-logo.png"
            alt=""
            aria-hidden="true"
            className={css({
              width: '14',
              height: '14',
              flexShrink: 0,
              objectFit: 'contain',
            })}
          />
          <div
            className={css({
              display: 'grid',
              gap: '0.5',
              minWidth: 0,
            })}
          >
            <span
              className={css({
                display: 'block',
                m: 0,
                fontSize: '2xl',
                lineHeight: '0.95',
                letterSpacing: '0.05em',
                fontWeight: '700',
                textTransform: 'uppercase',
                color: 'text.strong',
              })}
            >
              Sentinel Verify
            </span>
          </div>
        </div>
      </div>

      <div
        className={css({
          flex: '1',
          minHeight: 0,
          width: '100%',
          overflowY: 'auto',
          overflowX: 'hidden',
          scrollbarWidth: 'none',
          '&::-webkit-scrollbar': {
            display: 'none',
          },
          boxSizing: 'border-box',
          px: '3',
          py: '4',
        })}
      >
        <AsyncStateRenderer
          isLoading={isLoading}
          error={errorMessage}
          isEmpty={primeGroups.length === 0}
          loadingView={<SkeletonStack count={6} itemHeight={64} />}
          errorView={
            <ErrorState
              title="Unable to load primes"
              description="An error occurred while fetching primes data."
              errorMessage={errorMessage ?? undefined}
              tone="critical"
              size="inline"
            />
          }
          emptyView={
            <EmptyState
              title="No primes returned"
              description="No primes were returned by the API."
            />
          }
        >
          <div className={css({ display: 'grid', gap: '2.5' })}>
            {primeGroups.map((primeGroup) => {
              const isSelected = primeGroup.key === selectedPrimeId;
              return (
                <button
                  key={primeGroup.key}
                  type="button"
                  aria-pressed={isSelected}
                  disabled={primeButtonsDisabled}
                  onClick={() => onSelectPrime(primeGroup.key)}
                  className={css({
                    width: '100%',
                    boxSizing: 'border-box',
                    textAlign: 'left',
                    borderRadius: 'md',
                    borderWidth: '1px',
                    borderStyle: 'solid',
                    borderColor: isSelected
                      ? 'interactive.accent'
                      : 'border.subtle',
                    bg: isSelected ? 'interactive.selected' : 'surface.default',
                    px: '3.5',
                    py: '3.5',
                    cursor: 'pointer',
                    transitionDuration: 'fast',
                    transitionProperty:
                      'background-color, border-color, transform',
                    _hover: {
                      // Neutral grey wash, not accent-tinted `interactive.hover`
                      // — see DESIGN.md, "The Signal Budget Rule".
                      bg: 'surface.hover',
                      transform: 'translateY(-1px)',
                    },
                    _disabled: {
                      cursor: 'not-allowed',
                      opacity: 0.45,
                      _hover: { bg: 'surface.default', transform: 'none' },
                    },
                  })}
                >
                  <div className={flex({ align: 'center', gap: '3.5' })}>
                    <ProtocolLogo
                      protocolName={primeGroup.name}
                      isSelected={isSelected}
                      size="8"
                    />
                    <div
                      className={css({
                        display: 'grid',
                        gap: '0.5',
                        minWidth: 0,
                      })}
                    >
                      <div
                        className={flex({
                          align: 'baseline',
                          gap: '1.5',
                          minWidth: 0,
                        })}
                      >
                        <p
                          className={css({
                            m: 0,
                            fontSize: 'sm',
                            fontWeight: 'semibold',
                            color: 'text.strong',
                          })}
                        >
                          {primeGroup.name}
                        </p>
                        <span
                          className={css({
                            fontSize: 'xs',
                            color: 'text.muted',
                            whiteSpace: 'nowrap',
                          })}
                        >
                          {primeGroup.chainCount}{' '}
                          {primeGroup.chainCount === 1 ? 'chain' : 'chains'}
                        </span>
                      </div>
                      <span
                        className={css({
                          fontFamily: 'mono',
                          fontSize: 'xs',
                          color: 'text.link',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                          display: 'block',
                        })}
                        title={primeGroup.vaultAddress ?? undefined}
                      >
                        {truncateMiddle(primeGroup.vaultAddress, 6, 4)}
                      </span>
                    </div>
                  </div>
                </button>
              );
            })}
          </div>
        </AsyncStateRenderer>
      </div>

      <div
        className={css({
          width: '100%',
          boxSizing: 'border-box',
          px: '5',
          pt: '3',
        })}
      >
        <Switch.Root
          checked={showAllPrimes}
          disabled={!canShowAllPrimes}
          onCheckedChange={(details: { checked: boolean }) =>
            onShowAllPrimesChange(details.checked)
          }
          className={flex({
            align: 'center',
            justify: 'space-between',
            gap: '3',
            width: '100%',
            cursor: canShowAllPrimes ? 'pointer' : 'not-allowed',
            opacity: canShowAllPrimes ? 1 : 0.5,
          })}
        >
          <Switch.Label
            className={css({
              fontSize: 'sm',
              color: 'text.muted',
            })}
          >
            Show all primes
          </Switch.Label>
          <Switch.Control className={switchStyles.root}>
            <Switch.Thumb className={switchStyles.thumb} />
          </Switch.Control>
          <Switch.HiddenInput />
        </Switch.Root>
      </div>

      <div
        className={css({
          width: '100%',
          boxSizing: 'border-box',
          px: '4',
          py: '3',
          bg: 'surface.default',
        })}
      >
        <div
          className={flex({
            align: 'center',
            justify: 'flex-start',
            gap: '3',
          })}
        >
          <ThemeToggle />
        </div>
      </div>
    </div>
  );
}
