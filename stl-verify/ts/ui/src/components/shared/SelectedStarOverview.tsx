import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import type { Star } from '../../types/allocation';

type SelectedStarOverviewProps = {
  selectedStar: Star | null;
  isLoading: boolean;
  errorMessage: string | null;
};

function formatAddress(address: string): string {
  if (address.length <= 18) {
    return address;
  }

  return `${address.slice(0, 10)}…${address.slice(-6)}`;
}

function SurfaceCard({ title, body }: { title: string; body: string }) {
  return (
    <article
      className={css({
        borderRadius: '2xl',
        borderWidth: '1px',
        borderStyle: 'solid',
        borderColor: 'border.subtle',
        bg: 'surface.default',
        p: '5',
      })}
    >
      <h3
        className={css({
          m: 0,
          fontSize: 'sm',
          textTransform: 'uppercase',
          letterSpacing: '0.12em',
          color: 'text.muted',
        })}
      >
        {title}
      </h3>
      <p
        className={css({
          m: 0,
          mt: '3',
          fontSize: 'sm',
          lineHeight: '1.7',
          color: 'text.default',
        })}
      >
        {body}
      </p>
    </article>
  );
}

export function SelectedStarOverview({
  selectedStar,
  isLoading,
  errorMessage,
}: SelectedStarOverviewProps) {
  const headline = isLoading
    ? 'Loading registered stars'
    : selectedStar
      ? selectedStar.name
      : 'Select a star to continue';

  const supportingCopy = isLoading
    ? 'The shell is live and waiting for the stars endpoint to resolve.'
    : selectedStar
      ? 'Stage 4 is complete: star selection, theme persistence, URL state, and shared Archon client wiring are in place. The allocations grid and lower risk panels follow next.'
      : 'Pick a star from the sidebar to validate URL persistence and the linked design-system shell.';

  return (
    <div
      className={css({
        minHeight: '100%',
        bg: 'surface.subtle',
        px: { base: '5', md: '7' },
        py: { base: '6', md: '7' },
      })}
    >
      <section
        className={css({
          borderRadius: '3xl',
          borderWidth: '1px',
          borderStyle: 'solid',
          borderColor: 'border.subtle',
          bg: 'surface.default',
          px: { base: '5', md: '7' },
          py: { base: '6', md: '7' },
          boxShadow: '0 24px 80px rgba(15, 23, 42, 0.08)',
        })}
      >
        <div
          className={flex({
            justify: 'space-between',
            align: 'flex-start',
            gap: '6',
            wrap: 'wrap',
          })}
        >
          <div className={css({ display: 'grid', gap: '3', maxWidth: '3xl' })}>
            <p
              className={css({
                m: 0,
                fontSize: 'xs',
                textTransform: 'uppercase',
                letterSpacing: '0.18em',
                color: 'text.muted',
              })}
            >
              Stage 4 complete
            </p>
            <h2
              className={css({
                m: 0,
                fontSize: { base: '3xl', md: '5xl' },
                lineHeight: '0.95',
                color: 'text.strong',
                maxWidth: '4xl',
              })}
            >
              {headline}
            </h2>
            <p
              className={css({
                m: 0,
                maxWidth: '2xl',
                fontSize: 'md',
                lineHeight: '1.8',
                color: 'text.default',
              })}
            >
              {supportingCopy}
            </p>
          </div>

          <div
            className={css({
              minWidth: '16rem',
              borderRadius: '2xl',
              borderWidth: '1px',
              borderStyle: 'solid',
              borderColor: 'border.subtle',
              bg: 'surface.subtle',
              p: '4',
            })}
          >
            <p className={css({ m: 0, fontSize: 'xs', color: 'text.muted' })}>
              Active selection
            </p>
            <p
              className={css({
                m: 0,
                mt: '2',
                fontSize: 'lg',
                color: 'text.strong',
              })}
            >
              {selectedStar ? selectedStar.name : 'No star selected'}
            </p>
            <p
              className={css({
                m: 0,
                mt: '1',
                fontSize: 'sm',
                color: 'text.muted',
              })}
            >
              {selectedStar
                ? formatAddress(selectedStar.address)
                : 'Waiting for sidebar input'}
            </p>
          </div>
        </div>

        {errorMessage ? (
          <div
            className={css({
              mt: '5',
              borderRadius: '2xl',
              borderWidth: '1px',
              borderStyle: 'solid',
              borderColor: 'border.default',
              bg: 'surface.subtle',
              px: '4',
              py: '3.5',
            })}
          >
            <p className={css({ m: 0, fontSize: 'sm', color: 'text.strong' })}>
              API error surfaced during Phase 4 validation.
            </p>
            <p
              className={css({
                m: 0,
                mt: '1.5',
                fontSize: 'sm',
                color: 'text.muted',
              })}
            >
              {errorMessage}
            </p>
          </div>
        ) : null}
      </section>

      <section
        className={css({
          mt: '6',
          display: 'grid',
          gridTemplateColumns: { base: '1fr', xl: 'repeat(3, minmax(0, 1fr))' },
          gap: '4',
        })}
      >
        <SurfaceCard
          title="Shell"
          body="The app now consumes SidebarLayout, ThemeProvider, and the shared Archon HTTP client packages from the local stl-ui worktree, so upstream UI and client changes are immediately available here."
        />
        <SurfaceCard
          title="Persistence"
          body="Star selection is stored in the URL with lightweight browser-state sync, ready to expand for network, protocol, tab, and receipt token state in later phases."
        />
        <SurfaceCard
          title="Next"
          body="Phase 5 adds allocations, filter controls, and row selection. Phase 6 attaches the resizable lower panel for risk breakdown and bad debt tabs."
        />
      </section>
    </div>
  );
}
