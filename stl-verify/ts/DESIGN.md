---
name: STL Verify UI
description: Dense risk-operations interface optimized for fast scanning and confident drill-down.
colors:
  colors.surface.canvas.light: "#fafafa"
  colors.surface.canvas.dark: "#0a0a0a"
  colors.surface.default.light: "#fff"
  colors.surface.default.dark: "#171717"
  colors.surface.subtle.light: "#f5f5f5"
  colors.surface.subtle.dark: "#262626"
  colors.surface.hover.light: "#f5f5f5"
  colors.surface.hover.dark: "#404040"
  colors.border.hairline.light: "rgba(9, 9, 11, 0.06)"
  colors.border.hairline.dark: "rgba(255, 255, 255, 0.08)"
  colors.border.subtle.light: "#d4d4d4"
  colors.border.subtle.dark: "#404040"
  colors.border.default.light: "#a3a3a3"
  colors.border.default.dark: "#525252"
  colors.border.strong.light: "#737373"
  colors.border.strong.dark: "#737373"
  colors.text.strong.light: "#0a0a0a"
  colors.text.strong.dark: "#fff"
  colors.text.default.light: "#171717"
  colors.text.default.dark: "#f5f5f5"
  colors.text.muted.light: "#737373"
  colors.text.muted.dark: "#a3a3a3"
  colors.text.subtle.light: "#737373"
  colors.text.subtle.dark: "#a3a3a3"
  colors.text.inverse.light: "#fafafa"
  colors.text.inverse.dark: "#fafafa"
  colors.text.link.light: "#2563eb"
  colors.text.link.dark: "#93c5fd"
  colors.text.interactive.light: "#2563eb"
  colors.text.interactive.dark: "#93c5fd"
  colors.text.critical.light: "#dc2626"
  colors.text.critical.dark: "#fca5a5"
  colors.text.violet.light: "#6d28d9"
  colors.text.violet.dark: "#c4b5fd"
  colors.interactive.hover.light: "#eff6ff"
  colors.interactive.hover.dark: "#172554"
  colors.interactive.selected.light: "#dbeafe"
  colors.interactive.selected.dark: "#1e3a8a"
  colors.interactive.accent.light: "#2563eb"
  colors.interactive.accent.dark: "#93c5fd"
  colors.bg.neutral.light: "#f5f5f5"
  colors.bg.neutral.dark: "#262626"
  colors.bg.violet.light: "#f5f3ff"
  colors.bg.violet.dark: "#2e1065"
  colors.overlay.backdrop.light: "rgb(15 23 42 / 0.28)"
  colors.overlay.backdrop.dark: "rgb(3 7 18 / 0.48)"
  colors.overlay.tooltip.light: "rgb(15 23 42 / 0.96)"
  colors.overlay.tooltip.dark: "rgb(3 7 18 / 0.96)"
  colors.chart.series.primary.light: "#2563eb"
  colors.chart.series.primary.dark: "#93c5fd"
  colors.chart.series.secondary.light: "#0d9488"
  colors.chart.series.secondary.dark: "#5eead4"
  colors.chart.series.tertiary.light: "#7c3aed"
  colors.chart.series.tertiary.dark: "#c4b5fd"
  colors.chart.series.quaternary.light: "#d97706"
  colors.chart.series.quaternary.dark: "#fcd34d"
  colors.chart.series.quinary.light: "#ea580c"
  colors.chart.series.quinary.dark: "#fdba74"
typography:
  display:
    fontFamily: "IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif"
    fontSize: "2.25rem"
    fontWeight: 400
    lineHeight: 1.25
    letterSpacing: "normal"
  headline:
    fontFamily: "IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif"
    fontSize: "1.5rem"
    fontWeight: 600
    lineHeight: 1.25
    letterSpacing: "normal"
  title:
    fontFamily: "IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif"
    fontSize: "1rem"
    fontWeight: 600
    lineHeight: 1.5
    letterSpacing: "normal"
  body:
    fontFamily: "IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: 1.4
    letterSpacing: "normal"
  label:
    fontFamily: "IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 600
    lineHeight: 1.5
    letterSpacing: "0.04em"
  mono:
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace'
    fontSize: "0.6875rem"
    fontWeight: 400
    lineHeight: 1.5
    letterSpacing: "normal"
# The steps this app actually reaches for, out of the preset `radii` scale
# (xs / sm / md / lg / xl / 2xl / 3xl / 4xl / full). There is no `pill` token;
# 9999px is `radii.full`.
rounded:
  xs: "0.125rem"
  sm: "0.25rem"
  md: "0.375rem"
  lg: "0.5rem"
  full: "9999px"
# Non-normative: these are prose names for the spacing rhythm, not Panda tokens.
# Do not grep for them in styled-system; use the numeric spacing scale.
spacing:
  tight: "0.75rem"
  compact: "1rem"
  comfortable: "1.25rem"
  section: "1.5rem"
  panel: "2rem"
# Geometry and role references only, quoted in the LIGHT theme for brevity.
# Every colour below is a semantic token that flips on its own; read the dark
# value from the colors block above rather than adding a second component spec.
components:
  # `button` recipe, emphasis="solid" size="md". The fill comes from the
  # colorPalette solid role tokens, not from a text/surface token.
  button-primary:
    backgroundColor: "{colors.colorPalette.solid.bg}"
    textColor: "{colors.colorPalette.solid.fg}"
    rounded: "{rounded.md}"
    padding: "0 0.625rem" # recipe sets px only; height does the vertical work
    height: "2rem"
  button-primary-hover:
    backgroundColor: "{colors.colorPalette.solid.bgHover}"
    textColor: "{colors.colorPalette.solid.fg}"
  # `button` recipe defaults (variant="panel", no emphasis) — the low-emphasis
  # action. There is no `quiet` variant.
  button-panel:
    backgroundColor: "{colors.surface.default.light}"
    textColor: "{colors.text.default.light}"
    borderColor: "{colors.border.subtle.light}"
    rounded: "{rounded.md}"
    padding: "0 0.625rem"
    height: "2rem"
  # `input` / `select` / `searchInput` recipes all share these metrics.
  field-input:
    backgroundColor: "{colors.surface.default.light}"
    textColor: "{colors.text.default.light}"
    rounded: "{rounded.md}"
    padding: "0 0.75rem"
    height: "2.25rem"
  # Local to this app: the allocation category chip in `AllocationGrid`. Not a
  # design-system recipe.
  chip-category:
    backgroundColor: "{colors.colorPalette.subtle.bg}"
    textColor: "{colors.colorPalette.subtle.fg}"
    rounded: "{rounded.md}"
    padding: "0.25rem 0.5rem"
  table-row-selected:
    backgroundColor: "{colors.interactive.selected.light}"
    textColor: "{colors.text.default.light}"
---

# Design System: STL Verify

## Overview

**Creative North Star: "Calm Control Room"**

STL Verify should feel like an operations desk built for sustained concentration, not a presentation layer. Information density is high, but the layout keeps scanning friction low by using stable structure, restrained color, and predictable interaction zones.

The visual system favors practical rhythm over decorative flourish. High-value metrics, filters, and tables are separated through spacing cadence and tonal layers, while interaction states stay quiet until users need to act. The interface rejects ornamental gradients, novelty controls, and high-chroma noise that competes with risk signals.

Token provenance for this spec comes from `@archon-research/design-system/panda-preset`, which owns the surface, border, text, interactive, `bg.*`, `chart.*`, shadow, and scrollbar semantic ramps. `stl-verify/ts/ui/panda.config.ts` adds **only** what the preset does not ship — `text.subtle`, `text.inverse`, `text.violet`, `bg.neutral`, `bg.violet`, `overlay.backdrop`, `overlay.tooltip`, `interactive.accent`, `chart.series.quaternary`, `chart.series.quinary` — and shadows nothing. Every hex in the frontmatter below is a resolved value read out of `npx panda cssgen` output, not a hand-maintained copy; regenerate and re-read after any preset upgrade.

**Do not re-add local overrides of preset tokens.** A local copy silently reverts upstream token fixes: the previous config redefined `surface.default` to `gray.950` in dark mode, which made a raised panel the darkest thing on the page and inverted the elevation ramp.

### Neutral hue

The neutral ramp is **achromatic** (`neutral.*`), not blue-tinted. `gray.*` is a separate, blue-tinted raw palette family that still exists in `@pandacss/preset-panda`; mixing the two puts a visible hue split wherever the families abut. The per-channel gap runs from 1 to 17 points depending on the step — at the mid-dark end it is wide (`gray.800` #1f2937 vs `neutral.800` #262626 differs by 7/3/17 on R/G/B), which reads as a cool cast beside an achromatic surface. Reach for semantic tokens, and when a raw step is unavoidable use `neutral.*`.

**Key Characteristics:**
- Dense by design, readable at a glance.
- Neutral-first surfaces with one operational accent.
- Structured spacing rhythm from compact controls to roomy section breaks.
- Familiar controls and table affordances over experimental patterns.

## Colors

The palette is restrained and operational: achromatic neutrals carry most surfaces, blue accent is reserved for action and focus. Semantic tokens are the source of truth and are applied as light/dark pairs.

### Primary
- **colors.text.interactive** (#2563eb / #93c5fd): the canonical accent — actionable text, focus emphasis, primary fills.
- **colors.text.link** (#2563eb / #93c5fd): navigation that leaves the current view (explorer links, external docs), so it reads differently in intent from a control accent even where the value matches.
- **colors.interactive.accent** (#2563eb / #93c5fd): a local addition that stays defined regardless of what upstream does. App code uses it as the accent across the sidebar, addresses, methodology panel, bottom panel, and activity feed; independently, the shipped `ErrorState`, `ErrorBoundary`, and `RangePicker` read `var(--colors-interactive-accent, <hex>)` from inline styles, and the preset defines no such token — undefined, those three fall back to a hardcoded light-mode blue in both themes. Its value is pinned to `text.interactive` so the app has one accent, not two; either token is correct in new code.

### Surface ramp (luminance-ordered, both themes)
The ramp steps forward monotonically in both themes: canvas is furthest back, panels sit on top of it, insets are a distinct well inside a panel.

- **colors.surface.canvas** (#fafafa / #0a0a0a): the application background. This is what `src/index.css` paints on `body`.
- **colors.surface.default** (#fff / #171717): main panel and control surface — lighter than the canvas in **both** themes.
- **colors.surface.subtle** (#f5f5f5 / #262626): recessed insets *within* a panel. Not the page background.
- **colors.surface.hover** (#f5f5f5 / #404040): the neutral grey wash for row and list-item hover. **In this app's own styles**, use this rather than `interactive.hover`.
- **colors.interactive.hover** (#eff6ff / #172554) and **colors.interactive.selected** (#dbeafe / #1e3a8a): accent-tinted, so selection and accent-hover read as *active*. Applying these to an ordinary row hover turns the whole table blue. Note this is an app-level rule, not a package-level one: five preset recipes (`button`, `searchInput`, `segmentedControl`, `interactiveItem`, `drawer`) use `interactive.hover` internally, and `segmentedControl` is one this app calls directly — so an accent-tinted hover does ship in places, and the rule is about styles we write ourselves.

### Borders and text
- **colors.border.hairline** (rgba(9, 9, 11, 0.06) / rgba(255, 255, 255, 0.08)): ~6% alpha dividers and insets, for stacked internal rules where a solid border becomes visual noise. Its only call site is `MethodologyPanel`. Grid row separators are **not** hairline: the `dataTable` recipe draws both its frame and its row rules in `border.subtle`, and overriding that per-row would fight the recipe.
- **colors.border.subtle** (#d4d4d4 / #404040): component edges — input, panel, and card strokes.
- **colors.border.default** (#a3a3a3 / #525252): stronger state borders.
- **colors.border.strong** (#737373 / #737373): selection emphasis and selected-row outlines.
- **colors.text.strong** (#0a0a0a / #ffffff): strong titles and key labels.
- **colors.text.default** (#171717 / #f5f5f5): core body and table text.
- **colors.text.muted** (#737373 / #a3a3a3): metadata and supporting copy.
- **colors.text.subtle** (#737373 / #a3a3a3): the quietest meta tier. Currently pinned to `text.muted`'s value — anything lighter than `neutral.500` on white fails WCAG AA (`neutral.400` on white is 2.52:1), so a genuine fourth step has to be inserted *between* `default` and `muted`, not below `muted`.
- **colors.text.inverse** (#fafafa / #fafafa): theme-invariant light text, for always-dark fills such as `overlay.tooltip`.
- **colors.text.critical** (#dc2626 / #fca5a5): the absent state on a risk dashboard. Use it; a risk grid that signals only success/warning/interactive is under-reporting.

### Status fills
`bg.success`, `bg.warning`, `bg.critical` come from the preset; `bg.neutral` (#f5f5f5 / #262626) completes the status family locally, and `bg.violet` (#f5f3ff / #2e1065) is a status-free categorical fill added for the allocation category chips (see the token comment in `panda.config.ts` for why `colorPalette` cannot supply it).

`bg.*` is a *fill*; `surface.*` is a *layer*. Do not substitute one for the other. The preset does blur this itself in one place: it ships `bg.canvas` as an exact duplicate of `surface.canvas` (both `neutral.50` / `neutral.950`). Prefer `surface.canvas` for the page layer; `bg.canvas` is not a second, distinct value.

### Chart series
Ordinal order is `primary → secondary → tertiary → quaternary → quinary`; `positive` and `critical` are semantic and must not be used to fill out an ordinal scale.

- **primary** #2563eb / #93c5fd — **secondary** #0d9488 / #5eead4 — **tertiary** #7c3aed / #c4b5fd — **quaternary** #d97706 / #fcd34d — **quinary** #ea580c / #fdba74
- `quaternary` and `quinary` are local additions: the preset's ordinal scale stops at `tertiary` (filed upstream), and the only roles left (`positive`, `critical`) are semantic, so they would mis-signal on a routine capital/debt metric.
- Supporting chart tokens, all dark-aware: `chart.axis` (#737373 / #a3a3a3), `chart.grid` (#e5e5e5 / #404040), `chart.area.primary` (#dbeafe / #1e3a8a).

### Overlays
`overlay.backdrop` and `overlay.tooltip` are local: they are semi-transparent scrims and always-dark fills, which cannot be expressed as a step on the opaque surface ramp. The preset ships no `overlay.*` family.

### Named Rules
**The Signal Budget Rule.** In styles this app writes, accent blue is used only when an element is actionable, selected, or needs immediate operator attention. A row hover is not one of those — that is `surface.hover`. Preset recipes make their own call; do not "correct" them from the outside.

## Typography

**Display Font:** IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif  
**Body Font:** IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif  
**Label Font:** IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif  
**Mono Font** (`fonts.mono`, used for every on-chain address and hash): `ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace`

**Character:** Utility-first and technically neutral. Typography supports quick parsing with minimal personality overhead.

### Hierarchy
The five roles below are **prose specs in this document only** — there is no `textStyles` block in `panda.config.ts`, so none of them is reachable from code as `textStyle: 'body'`. They describe the intended rhythm; the implementable vocabulary is the preset's `textStyles` (`panelTitle`, `sectionLabel`, `bodySm`, `metaText`, `microLabel`, `codeBlock`) plus the raw `fontSize`/`fontWeight` scales, which is what the app currently uses.

Two of the roles are close enough to a preset value to mislead if read as tokens: `label`'s 0.04em tracking sits between `letterSpacings.wide` (0.025em) and `wider` (0.05em), and `body`'s 1.4 line-height is not `lineHeights.relaxed` (1.625).

- **Display** (400, 2.25rem, 1.25): Prime name and highest-level panel titles.
- **Headline** (600, 1.5rem, 1.25): Section-level emphasis and major subheads.
- **Title** (600, 1rem, 1.5): Metric titles and table-adjacent headings.
- **Body** (400, 0.875rem, 1.4): Primary content rows, control values, and standard copy.
- **Label** (600, 0.75rem, 0.04em tracking): Tabs, chips, and compact metadata.

### Micro scale
The preset adds a step below `xs` (0.75rem) and **redefines** an existing one:

- `3xs` = 0.625rem (10px) — new; the floor for micro labels.
- `2xs` = **0.6875rem** (11px) — changed from Panda's 0.5rem (8px), a silent 37.5% growth that the compiler cannot flag. Existing `2xs` call sites must be eyeballed; most want `3xs` or the `microLabel` textStyle.

### Named Rules
**The Operator Scan Rule.** For data-heavy zones, keep body copy in the 0.75rem to 0.875rem range and reserve larger type for wayfinding anchors only.

## Elevation

Depth is mostly tonal rather than shadow-driven. Most surfaces remain flat, with subtle border contrast and background shifts carrying hierarchy. Shadows appear only in contained overlays and selected-detail emphasis.

### Shadow Vocabulary
Three shadow tokens are dark-aware — in dark mode they swap a light-mode drop shadow for a stronger drop plus a `rgba(255, 255, 255, 0.0x)` inset top highlight, which is what makes an edge read as raised on a near-black surface:

- **`shadows.elevation`** — the named raised-panel token. Light: `0 1px 2px 0 rgba(15, 23, 42, 0.08), 0 1px 3px 0 rgba(15, 23, 42, 0.06)`. Dark: `0 1px 2px 0 rgba(0, 0, 0, 0.55), inset 0 1px 0 0 rgba(255, 255, 255, 0.06)`. Prefer this over `sm` for panels.
- **`shadows.xs`** — light `0 1px 2px 0 rgba(15, 23, 42, 0.06)`; dark `0 1px 2px 0 rgba(0, 0, 0, 0.5), inset 0 1px 0 0 rgba(255, 255, 255, 0.05)`. Correct choice for small controls such as a slider thumb.
- **`shadows.sm`** — light `0 1px 3px 0 rgba(15, 23, 42, 0.10), 0 1px 2px -1px rgba(15, 23, 42, 0.10)`; dark `0 2px 4px 0 rgba(0, 0, 0, 0.6), inset 0 1px 0 0 rgba(255, 255, 255, 0.06)`.

**`md` / `lg` / `xl` / `2xl` are theme-blind** — they keep Panda's `rgb(0 0 0 / 0.1)`–`0.25` defaults with no `.dark` override, so a `2xl` on a `#171717` drawer is effectively invisible. This is an upstream gap, filed. Until it is fixed, treat a large shadow on an overlay as decoration and carry the layering with `border.subtle` plus the surface ramp.

Never hand-write a shadow literal: a `rgba(0, 0, 0, 0.2)` drop shadow disappears on a dark surface, which is exactly the failure the dark-aware tokens exist to prevent.

### Named Rules
**The Flat-by-Default Rule.** Base states stay flat, elevation appears only to clarify layering or active context.

## Components

### Buttons
- **Shape:** Rounded rectangle, `rounded.md` (0.375rem) — the `button` recipe's only radius, across every variant and size.
- **Metrics:** the default `size="md"` is 2rem tall with 0.625rem horizontal padding and no vertical padding (height carries it). `size="lg"` is the 2.25rem step that matches the field controls; reach for it when a button sits in a row with an input or select.
- **Primary:** `emphasis="solid"`, which fills from `colorPalette.solid.bg` with `colorPalette.solid.fg` text — set `colorPalette="blue"` for the CTA, `"red"` for destructive. Optimized for control bars.
- **Hover / Focus:** `colorPalette.solid.bgHover` plus visible border/focus intent.
- **Low emphasis:** the recipe default (`variant="panel"`, no `emphasis`): `surface.default` on a `border.subtle` stroke. There is no `quiet` variant.

### Chips
- **Style:** `rounded.md` (0.375rem), not pill, and no `textTransform` — the label is sentence-cased at whatever the source gives.
- **Colour:** a category chip fills from `colorPalette.subtle.bg` / `.subtle.fg`, so it stays dark-aware across every hue that has role sub-tokens. The one exception is the local `bg.violet` / `text.violet` pair, which covers the hue `colorPalette` cannot.
- **State:** used to mark section context and compact taxonomy labels. A status chip drives colour through `colorPalette` + `variant`, not through the deprecated `tone` prop.

### Cards / Containers
- **Corner Style:** `rounded.md` (0.375rem), uniformly. Radius does not vary with hierarchy depth — `panel`, `panelSection`, `statTile`, `surfaceMessage`, and the `dataTable` frame are all `md`, and matching them is what makes nested surfaces read as one system.
- **Background:** `surface.default` for primary panels, `surface.canvas` behind them, `surface.subtle` for recessed insets inside a panel.
- **Shadow Strategy:** Minimal, only for main section framing and overlays.
- **Border:** 1px neutral stroke for edge definition.
- **Internal Padding:** 1rem to 2rem, scaled by content density.

### Inputs / Fields
- **Style:** `surface.default` controls with a `border.subtle` stroke and `rounded.md` (0.375rem) corners; `input`, `select`, and `searchInput` all share those and a 2.25rem height.
- **Focus:** Accent and border emphasis, no decorative glow.
- **Error / Disabled:** Muted contrast and reduced emphasis, preserving readability.

### Navigation
- **Style:** Sidebar plus top filter bar with explicit grouping and compact control heights.
- **State:** Active entities are highlighted by fill or border, not animation-heavy treatments.
- **Mobile Treatment:** Structural stacking before micro-adjustment, controls stay full-width when compressed.

### Signature Component
- **Allocation grid + summary metric row:** a combined pattern where a 4-up row of `SummaryMetric` tiles sits directly above `AllocationGrid`, keeping summary and evidence in one scan path. Both are app components; neither is a design-system export.

## Do's and Don'ts

### Do:
- **Do** keep operational accent usage intentional, primary actions and active states only.
- **Do** preserve consistent control heights across a row: `input`, `select`, and `searchInput` are all 2.25rem, so a button beside them wants `size="lg"` rather than the 2rem default.
- **Do** use spacing rhythm to separate groups, tight inside controls and generous between sections.
- **Do** keep table readability primary, metadata muted but legible.

### Don't:
- **Don't** introduce decorative gradients, neon accents, or glassmorphism overlays.
- **Don't** use side-stripe border accents on cards, list rows, or callouts.
- **Don't** apply large-display typography to control labels or dense data regions.
- **Don't** replace familiar table and filter affordances with novelty interactions.

### Don't (token discipline):
- **Don't** override a preset semantic token in `panda.config.ts`. Local `theme.extend` merges last and wins, so a copy silently reverts upstream fixes. Add only tokens the preset does not ship, and say in a comment why.
- **Don't** write a raw hex, `rgba()`, or a `var(--colors-*)` read in application code. Both fail the theme switch, and a raw `var()` read also bypasses any future token rename.
- **Don't** reach for `gray.*`, `neutral.*`, or any other raw palette step where a semantic token exists.
- **Don't** compute a token path in a helper and pass it to `css()` as a variable. Panda extracts from **source text**: `bg: getCategoryColor(c)` emits no declaration at all and fails silently. Use a recipe variant, or index a literal lookup inside the `css()` call.
- **Don't** name a token that does not exist. Panda passes an unknown path through as a literal CSS value, the browser discards the declaration, and the element inherits instead. Sweep for it: `npx panda cssgen --outfile /tmp/x.css && grep -oE '^\s+[a-z-]+: [a-z]+\.[a-zA-Z.]+;' /tmp/x.css` must be empty.
- **Don't** add an unlayered rule to `src/index.css`. Unlayered CSS beats every layered rule regardless of specificity, so a bare `button { font: inherit }` outranks `@layer recipes` and strips the type step from every design-system control. Everything in that file belongs inside `@layer base`.

## Last refreshed from

- `stl-verify/ts/ui/panda.config.ts` — the local `theme.extend` (additions only).
- `stl-verify/ts/ui/src/index.css` — the `@layer base` app reset and page background.
- `npx panda cssgen` output against `@archon-research/design-system@0.8.0-rohit-ds-tokens-and-primitives.1` — every resolved hex, font stack, radius, and shadow quoted above.
- `@archon-research/design-system/panda-preset` (`dist/panda-preset.js`) — semantic ramp intent and release notes.
