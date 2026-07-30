---
name: STL Verify UI
description: Dense risk-operations interface optimized for fast scanning and confident drill-down.
colors:
  colors.surface.canvas.light: "#fafafa"
  colors.surface.canvas.dark: "#0a0a0a"
  colors.surface.default.light: "#ffffff"
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
  colors.text.strong.dark: "#ffffff"
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
  colors.interactive.hover.light: "#eff6ff"
  colors.interactive.hover.dark: "#172554"
  colors.interactive.selected.light: "#dbeafe"
  colors.interactive.selected.dark: "#1e3a8a"
  colors.interactive.accent.light: "#2563eb"
  colors.interactive.accent.dark: "#93c5fd"
  colors.bg.info.light: "#eff6ff"
  colors.bg.info.dark: "#172554"
  colors.bg.neutral.light: "#f5f5f5"
  colors.bg.neutral.dark: "#262626"
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
rounded:
  sm: "0.25rem"
  md: "0.375rem"
  lg: "0.5rem"
  pill: "9999px"
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
  button-primary:
    backgroundColor: "{colors.text.interactive.light}"
    textColor: "{colors.surface.default.light}"
    rounded: "{rounded.lg}"
    padding: "0.5rem 0.875rem"
    height: "2.25rem"
  button-primary-hover:
    backgroundColor: "{colors.colorPalette.solid.bgHover}" # colorPalette=blue; not a raw hex
    textColor: "{colors.surface.default.light}"
  button-quiet:
    backgroundColor: "{colors.surface.hover.light}"
    textColor: "{colors.text.default.light}"
    rounded: "{rounded.lg}"
    padding: "0.5rem 0.875rem"
    height: "2.25rem"
  field-input:
    backgroundColor: "{colors.surface.default.light}"
    textColor: "{colors.text.default.light}"
    rounded: "{rounded.lg}"
    padding: "0 0.75rem"
    height: "2.25rem"
  chip-metric:
    backgroundColor: "{colors.bg.neutral.light}"
    textColor: "{colors.text.muted.light}"
    rounded: "{rounded.pill}"
    padding: "0.25rem 0.75rem"
  table-row-selected:
    backgroundColor: "{colors.interactive.selected.light}"
    textColor: "{colors.text.default.light}"
---

# Design System: STL Verify

## Overview

**Creative North Star: "Calm Control Room"**

STL Verify should feel like an operations desk built for sustained concentration, not a presentation layer. Information density is high, but the layout keeps scanning friction low by using stable structure, restrained color, and predictable interaction zones.

The visual system favors practical rhythm over decorative flourish. High-value metrics, filters, and tables are separated through spacing cadence and tonal layers, while interaction states stay quiet until users need to act. The interface rejects ornamental gradients, novelty controls, and high-chroma noise that competes with risk signals.

Token provenance for this spec comes from `@archon-research/design-system/panda-preset`, which owns the surface, border, text, interactive, `bg.*`, `chart.*`, shadow, and scrollbar semantic ramps. `stl-verify/ts/ui/panda.config.ts` adds **only** what the preset does not ship — `text.subtle`, `text.inverse`, `bg.info`, `bg.neutral`, `overlay.backdrop`, `overlay.tooltip`, `interactive.accent`, `chart.series.quaternary`, `chart.series.quinary` — and deliberately shadows nothing. Every hex in the frontmatter below is a resolved value read out of `npx panda cssgen` output, not a hand-maintained copy; regenerate and re-read after any preset upgrade.

**Do not re-add local overrides of preset tokens.** A local copy silently reverts upstream token fixes: the previous config redefined `surface.default` to `gray.950` in dark mode, which made a raised panel the darkest thing on the page and inverted the elevation ramp.

### Neutral hue

The neutral ramp is **achromatic** (`neutral.*`), not blue-tinted. `gray.*` is a separate, blue-tinted raw palette family that still exists in `@pandacss/preset-panda`; mixing the two puts a 1–3 point per-channel hue split wherever the families abut. Reach for semantic tokens, and when a raw step is unavoidable use `neutral.*`.

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
- **colors.interactive.accent** (#2563eb / #93c5fd): compatibility token only. `ErrorState`, `ErrorBoundary`, and `RangePicker` read `var(--colors-interactive-accent)` from inline styles and the preset does not define it, so this must stay defined until upstream ships it. New code uses `text.interactive`.

### Surface ramp (luminance-ordered, both themes)
The ramp steps forward monotonically in both themes: canvas is furthest back, panels sit on top of it, insets are a distinct well inside a panel.

- **colors.surface.canvas** (#fafafa / #0a0a0a): the application background. This is what `src/index.css` paints on `body`.
- **colors.surface.default** (#ffffff / #171717): main panel and control surface — lighter than the canvas in **both** themes.
- **colors.surface.subtle** (#f5f5f5 / #262626): recessed insets *within* a panel. Not the page background.
- **colors.surface.hover** (#f5f5f5 / #404040): the neutral grey wash for row and list-item hover. Use this, not `interactive.hover`.
- **colors.interactive.hover** (#eff6ff / #172554) and **colors.interactive.selected** (#dbeafe / #1e3a8a): accent-tinted, so selection and accent-hover read as *active*. Applying these to an ordinary row hover turns the whole table blue.

### Borders and text
- **colors.border.hairline** (rgba(9, 9, 11, 0.06) / rgba(255, 255, 255, 0.08)): ~6% alpha dividers and insets. Preferred for grid row separators and feed dividers, where a solid border is visual noise at 1000+ rows.
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
`bg.success`, `bg.warning`, `bg.critical` come from the preset; `bg.info` (#eff6ff / #172554) and `bg.neutral` (#f5f5f5 / #262626) are local additions completing the family. `bg.*` is a *fill*; `surface.*` is a *layer*. Do not substitute one for the other.

### Chart series
Ordinal order is `primary → secondary → tertiary → quaternary → quinary`; `positive` and `critical` are semantic and must not be used to fill out an ordinal scale.

- **primary** #2563eb / #93c5fd — **secondary** #0d9488 / #5eead4 — **tertiary** #7c3aed / #c4b5fd — **quaternary** #d97706 / #fcd34d — **quinary** #ea580c / #fdba74
- `quaternary` and `quinary` are local additions: the preset's ordinal scale stops at `tertiary` (filed upstream). They preserve the metric rail's historical amber/orange identity as dark-aware palette steps rather than the raw `#f59e0b` / `#f97316`.
- Supporting chart tokens, all dark-aware: `chart.axis` (#737373 / #a3a3a3), `chart.grid` (#e5e5e5 / #404040), `chart.area.primary` (#dbeafe / #1e3a8a).

### Overlays
`overlay.backdrop` and `overlay.tooltip` are local: they are semi-transparent scrims and always-dark fills, which cannot be expressed as a step on the opaque surface ramp. The preset ships no `overlay.*` family.

### Named Rules
**The Signal Budget Rule.** Accent blue is used only when an element is actionable, selected, or needs immediate operator attention. A row hover is not one of those — that is `surface.hover`.

## Typography

**Display Font:** IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif  
**Body Font:** IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif  
**Label Font:** IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif  
**Mono Font** (`fonts.mono`, used for every on-chain address and hash — 11 call sites): `ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace`

**Character:** Utility-first and technically neutral. Typography supports quick parsing with minimal personality overhead.

### Hierarchy
The five roles below are **local to this app**. None of them maps to a preset `textStyle`, and two are close enough to a preset value to mislead: `label`'s 0.04em tracking sits between `letterSpacings.wide` (0.025em) and `wider` (0.05em), and `body`'s 1.4 line-height is not `lineHeights.relaxed` (1.625). When reaching for a preset `textStyle` instead, the equivalents are `panelTitle`, `sectionLabel`, `bodySm`, `metaText`, and `microLabel`.

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
- **Shape:** Rounded rectangle with 0.5rem corners.
- **Primary:** Blue fill with white text, compact horizontal padding, optimized for control bars.
- **Hover / Focus:** Slight darkening of blue plus visible border/focus intent.
- **Quiet:** neutral grey wash (`surface.hover`) for low-priority actions and segmented controls — never the accent-tinted `interactive.hover`.

### Chips
- **Style:** Pill geometry on `bg.neutral` with uppercase tracking.
- **State:** Used to mark section context and compact taxonomy labels. A status chip drives colour through `colorPalette` + `variant`, not through the deprecated `tone` prop.

### Cards / Containers
- **Corner Style:** 0.375rem to 0.5rem based on hierarchy depth.
- **Background:** `surface.default` for primary panels, `surface.canvas` behind them, `surface.subtle` for recessed insets inside a panel.
- **Shadow Strategy:** Minimal, only for main section framing and overlays.
- **Border:** 1px neutral stroke for edge definition.
- **Internal Padding:** 1rem to 2rem, scaled by content density.

### Inputs / Fields
- **Style:** White controls with subtle neutral border and rounded 0.5rem corners.
- **Focus:** Accent and border emphasis, no decorative glow.
- **Error / Disabled:** Muted contrast and reduced emphasis, preserving readability.

### Navigation
- **Style:** Sidebar plus top filter bar with explicit grouping and compact control heights.
- **State:** Active entities are highlighted by fill or border, not animation-heavy treatments.
- **Mobile Treatment:** Structural stacking before micro-adjustment, controls stay full-width when compressed.

### Signature Component
- **Allocation Grid + Metric Rail:** A combined pattern where high-level metrics sit directly above the data table, keeping summary and evidence in one scan path.

## Do's and Don'ts

### Do:
- **Do** keep operational accent usage intentional, primary actions and active states only.
- **Do** preserve consistent control heights around 2.25rem across selects and inputs.
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
