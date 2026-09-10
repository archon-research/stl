# PREVIEW: STL Verify

How to run, preview, and visually verify the STL Verify UI. This app verifies through the running Vite app and its real screens. It does not use Ladle stories or automated visual snapshots; that infrastructure lives in the `@archon-research/uikit` preview package, not in this consumer.

## How to run
From `stl-verify/ts/ui`:
- Dev server: `npm run dev` (runs panda codegen first, then Vite).
- Production preview: `npm run build` then `npm run preview`.
- Type check: `npm run type:check`. Lint and format: `npm run lint`, `npm run format:check`.

If styles look stale, re-run `npm run prepare`.

## Screens and high-signal states
There is one primary screen (the allocation dashboard, `src/routes/AllocationRoute.tsx`), composed of:
- Prime sidebar (`PrimeSidebar`): prime selection, including the selected and active state.
- Top bar (`TopBar`): network and protocol filters plus allocation search; state is URL-synced (see `src/shared/lib/search-params.ts`).
- Summary metric row (`SummaryMetric`): the 4-up metric tiles above the grid.
- Allocation grid (`AllocationGrid`): the dense data table, including the selected-row state and the `Badge` category chips (`categorical.1..5` fills).
- Bottom panel (`BottomPanel`): segmented tabs for Risk breakdown, Required risk capital, and Activity.
- Activity feed (`tabs/ActivityFeed`): a `DataTable` — Time / Token / Action / Protocol / Amount / Block / Chain / Tx columns, plus the leading expander column — not the former card list. Each row expands in place through `renderDetailPanel` into the per-transaction protocol-event panel. The drawer-mode feed is the same table and the same columns, in a much narrower container.
- Risk detail drawer (`RiskDetailDrawer`): overlay drill-down for a selected allocation.

Verify each in both light and dark themes.

## Verification checkpoints
For any UI change, confirm:
1. Token integrity: colors, surfaces, borders, and text resolve via semantic tokens in both themes (no hardcoded values).
2. Selected and active states: sidebar selection, table selected row (inset outline), and segmented active tab render correctly.
3. Typography rhythm: table header casing and body density match `DESIGN.md`.
4. Mono addresses: token and prime addresses render in mono, in `text.link`, and truncate cleanly (`TokenAddress`, and the address line in `PrimeSidebar`).
5. Logos and fallbacks: `ProtocolLogo`, `ChainLogo`, `TokenLogo`, and `LogoAvatar` show correct fallbacks when an image is missing.
6. Overlays: drawer and tooltip use tokenized overlay colors and handle focus correctly. Tooltip bubbles come from the upstream `tooltip` recipe — a bubble rendering as bare unpositioned text means the recipe is missing, not mis-styled.
7. Empty, loading, and error: each data region degrades gracefully.
8. Loading placeholders: every region uses `SkeletonStack`/`SkeletonRows`, which now pulse by default. Check the pulse runs, that no skeleton is brighter than its resting state, and that it goes static under `prefers-reduced-motion`.
9. Row expansion (`ActivityFeed`): the expander column is discoverable, the detail panel loads per transaction, and expanding a second row leaves the first correct. Re-expanding a row already seen issues no request — the events are cached per tx hash for an hour, since a settled transaction's events do not change. A second request on re-expand means the cache key moved, or `staleTime`/`gcTime` on `CACHE.settledTx` dropped below the gap between expansions — the panel unmounts on collapse, so `gcTime` is what carries the entry across.
10. Styling coverage: `panda.config.ts` narrows `staticCss` to the recipes this app renders, so a newly imported design-system component renders **completely unstyled** until its recipe key is added. Any component that suddenly looks like unstyled HTML is this, not a token bug.

## Known visual-risk areas
- Segmented control active highlight (`BottomPanel`): repeatedly adjusted; verify the active tab fill and border after any change.
- Data table header typography and selected-row inset (`AllocationGrid`). The header voice is one shared override (`shared/ui/tableStyles.ts`) across all three tables — check them together.
- Activity feed as a table: column widths in the narrow drawer container, expander discoverability, the detail panel's fit inside a row, and the compact two-line time cell.
- Category chips: `Badge` padding around short labels, and the neutral fallback an unknown category renders as.
- Metric-card trend chart (`MetricCardChart` in `AllocationGrid`) first paint: width comes from `useContainerWidth`, so verify the pre-measure frame does not flash a wrong-width chart or jump the layout.
- Mono address formatting and truncation.
- Logo fallbacks and dashboard metadata hierarchy.
- Drawer and overlay layering and focus handling.

## When to add a new verification route
Add a dedicated preview route or fixture state when:
- A component has states that are hard to reach from live API data (rare errors, extreme values, empty sets).
- A regression recurs and you want the exact state captured so it is easy to re-check.
- A new screen or major component lands that the default dashboard flow does not exercise.

If automated visual regression becomes worthwhile, adopt the uikit preview and Ladle setup rather than reinventing it.

## Last refreshed from
- `ui/src/routes/`, `ui/src/features/` (`allocations/`, `activity/`), `ui/src/shared/` (`ui/`, `hooks/`, `lib/`)
- `ui/package.json` scripts, `ui/panda.config.ts`, and `DESIGN.md` — all three revised for design-system `0.9.0` (narrowed `staticCss`, upstream `tooltip` recipe, `DataTable` activity feed), so re-read them rather than trusting this list.
- `npx panda cssgen` output, for the resolved token values the checkpoints above refer to.
