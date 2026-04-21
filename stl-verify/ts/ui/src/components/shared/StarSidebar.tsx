import { ThemeToggle } from '@archon-research/design-system';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import type { Star } from '../../types/allocation';

type StarSidebarProps = {
  stars: Star[];
  selectedStarId: string | null;
  isLoading: boolean;
  errorMessage: string | null;
  onRetry: () => void;
  onSelectStar: (starId: string) => void;
};

function formatAddress(address: string): string {
  if (address.length <= 12) {
    return address;
  }

  return `${address.slice(0, 6)}…${address.slice(-4)}`;
}

function StarSkeletonList() {
  return (
    <div className={css({ display: 'grid', gap: '3' })}>
      {Array.from({ length: 6 }, (_, index) => (
        <div
          key={index}
          className={css({
            height: '16',
            borderRadius: 'md',
            borderWidth: '1px',
            borderStyle: 'solid',
            borderColor: 'border.subtle',
            bg: 'surface.subtle',
            opacity: 0.8,
          })}
        />
      ))}
    </div>
  );
}

export function StarSidebar({
  stars,
  selectedStarId,
  isLoading,
  errorMessage,
  onRetry,
  onSelectStar,
}: StarSidebarProps) {
  return (
    <div
      className={css({
        display: 'flex',
        flexDirection: 'column',
        minHeight: '100%',
        bg: 'surface.default',
      })}
    >
      <div
        className={css({
          px: '5',
          py: '5',
          borderBottomWidth: '1px',
          borderBottomStyle: 'solid',
          borderBottomColor: 'border.subtle',
        })}
      >
        <p
          className={css({
            m: 0,
            fontSize: 'xs',
            textTransform: 'uppercase',
            letterSpacing: '0.16em',
            color: 'text.muted',
          })}
        >
          Registered Stars
        </p>
        <h2
          className={css({
            m: 0,
            mt: '2',
            fontSize: '2xl',
            lineHeight: 'tight',
            color: 'text.strong',
          })}
        >
          STL Verify
        </h2>
        <p
          className={css({
            m: 0,
            mt: '2',
            fontSize: 'sm',
            color: 'text.muted',
          })}
        >
          Explore registered stars, compare live allocations, and inspect
          receipt-token risk from the focused position below.
        </p>
      </div>

      <div
        className={css({
          flex: '1',
          minHeight: 0,
          overflowY: 'auto',
          px: '3',
          py: '4',
        })}
      >
        {isLoading ? <StarSkeletonList /> : null}

        {!isLoading && errorMessage ? (
          <div
            className={css({
              borderRadius: 'md',
              borderWidth: '1px',
              borderStyle: 'solid',
              borderColor: 'border.default',
              bg: 'surface.subtle',
              p: '4',
            })}
          >
            <p className={css({ m: 0, fontSize: 'sm', color: 'text.strong' })}>
              Unable to load stars.
            </p>
            <p
              className={css({
                m: 0,
                mt: '2',
                fontSize: 'sm',
                color: 'text.muted',
              })}
            >
              {errorMessage}
            </p>
            <button
              type="button"
              onClick={onRetry}
              className={css({
                mt: '4',
                borderRadius: 'md',
                borderWidth: '1px',
                borderStyle: 'solid',
                borderColor: 'border.default',
                bg: 'surface.default',
                color: 'text.strong',
                px: '3.5',
                py: '2',
                cursor: 'pointer',
                _hover: { bg: 'interactive.hover' },
              })}
            >
              Retry request
            </button>
          </div>
        ) : null}

        {!isLoading && !errorMessage && stars.length === 0 ? (
          <div
            className={css({
              borderRadius: 'md',
              borderWidth: '1px',
              borderStyle: 'dashed',
              borderColor: 'border.default',
              bg: 'surface.subtle',
              p: '4',
            })}
          >
            <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
              No stars were returned by the API.
            </p>
          </div>
        ) : null}

        {!isLoading && !errorMessage && stars.length > 0 ? (
          <div className={css({ display: 'grid', gap: '2.5' })}>
            {stars.map((star) => {
              const isSelected = star.id === selectedStarId;
              return (
                <button
                  key={star.id}
                  type="button"
                  onClick={() => onSelectStar(star.id)}
                  className={css({
                    width: '100%',
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
                      bg: 'interactive.hover',
                      transform: 'translateY(-1px)',
                    },
                  })}
                >
                  <div
                    className={flex({
                      align: 'center',
                      justify: 'space-between',
                      gap: '3',
                    })}
                  >
                    <div
                      className={css({
                        width: '9',
                        height: '9',
                        borderRadius: 'full',
                        bg: isSelected
                          ? 'interactive.accent'
                          : 'surface.subtle',
                        color: isSelected ? 'white' : 'text.strong',
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        fontSize: 'xs',
                        fontWeight: 'semibold',
                        textTransform: 'uppercase',
                        flexShrink: 0,
                      })}
                    >
                      {star.name.slice(0, 2)}
                    </div>
                    <span
                      className={css({
                        fontSize: 'xs',
                        color: isSelected ? 'text.strong' : 'text.muted',
                      })}
                    >
                      {isSelected ? 'Selected' : 'Available'}
                    </span>
                  </div>
                  <p
                    className={css({
                      m: 0,
                      mt: '3',
                      fontSize: 'sm',
                      fontWeight: 'semibold',
                      color: 'text.strong',
                    })}
                  >
                    {star.name}
                  </p>
                  <p
                    className={css({
                      m: 0,
                      mt: '1',
                      fontSize: 'xs',
                      letterSpacing: '0.04em',
                      color: 'text.muted',
                    })}
                  >
                    {formatAddress(star.address)}
                  </p>
                </button>
              );
            })}
          </div>
        ) : null}
      </div>

      <div
        className={css({
          px: '4',
          py: '3',
          borderTopWidth: '1px',
          borderTopStyle: 'solid',
          borderTopColor: 'border.subtle',
          bg: 'surface.default',
        })}
      >
        <div
          className={flex({
            align: 'center',
            justify: 'space-between',
            gap: '3',
          })}
        >
          <span className={css({ fontSize: 'xs', color: 'text.muted' })}>
            Theme preference
          </span>
          <ThemeToggle />
        </div>
      </div>
    </div>
  );
}
