# PREVIEW: STL Verify

How to run, preview, and visually verify the STL Verify UI. This app verifies through the running Vite app and its real screens. It does not use Ladle stories or automated visual snapshots; that infrastructure lives in the `@archon-research/uikit` preview package, not in this consumer.

## How to run
From `stl-verify/ts/ui`:
- Dev server: `npm run dev` (runs panda codegen first, then Vite).
- Production preview: `npm run build` then `npm run preview`.
- Type check: `npm run type:check`. Lint and format: `npm run lint`, `npm run format:check`.

Token codegen (`panda codegen`) runs automatically via the `pre*` scripts. If styles look stale, re-run `npm run codegen:styles`.

## Screens and high-signal states
There is one primary screen (the allocation dashboard, `src/App.tsx`), composed of:
- Prime sidebar (`PrimeSidebar`): prime selection, including the selected and active state.
- Top bar (`TopBar`): network and protocol filters plus allocation search; state is URL-synced (see `src/lib/url-params.ts`).
- Summary metric rail (`SummaryMetric`): the 4-up metrics above the grid.
- Allocation grid (`AllocationGrid`): the dense data table, including the selected-row state.
- Bottom panel (`BottomPanel`): segmented tabs for Risk breakdown, Required risk capital, and Activity.
- Risk detail drawer (`RiskDetailDrawer`): overlay drill-down for a selected allocation.

Verify each in both light and dark themes.

## Verification checkpoints
For any UI change, confirm:
1. Token integrity: colors, surfaces, borders, and text resolve via semantic tokens in both themes (no hardcoded values).
2. Selected and active states: sidebar selection, table selected row (inset outline), and segmented active tab render correctly.
3. Typography rhythm: table header casing and body density match `DESIGN.md`.
4. Mono addresses: token and prime addresses render in mono and truncate cleanly (`Address`, `TokenAddress`).
5. Logos and fallbacks: `ProtocolLogo`, `ChainLogo`, `TokenLogo`, and `LogoAvatar` show correct fallbacks when an image is missing.
6. Overlays: drawer and tooltip use tokenized overlay colors and handle focus correctly.
7. Empty, loading, and error: each data region degrades gracefully.

## Known visual-risk areas
- Segmented control active highlight (`BottomPanel`): repeatedly adjusted; verify the active tab fill and border after any change.
- Data table header typography and selected-row inset (`AllocationGrid`).
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
- `src/App.tsx`, `src/components/` (`allocations/`, `shared/`), `src/data-table/`
- `package.json` scripts, `panda.config.ts`, `DESIGN.md`
- recent UI history (segmented control, table header typography, mono addresses, logo fallbacks)
