---
name: STL Verify UI
description: Dense risk-operations interface optimized for fast scanning and confident drill-down.
colors:
  colors.surface.default.light: "#ffffff"
  colors.surface.default.dark: "#030712"
  colors.surface.subtle.light: "#f9fafb"
  colors.surface.subtle.dark: "#111827"
  colors.surface.elevated.light: "#ffffff"
  colors.surface.elevated.dark: "#111827"
  colors.border.subtle.light: "#e5e7eb"
  colors.border.subtle.dark: "#1f2937"
  colors.border.default.light: "#d1d5db"
  colors.border.default.dark: "#374151"
  colors.text.muted.light: "#4b5563"
  colors.text.muted.dark: "#9ca3af"
  colors.text.default.light: "#374151"
  colors.text.default.dark: "#d1d5db"
  colors.text.strong.light: "#030712"
  colors.text.strong.dark: "#f9fafb"
  colors.interactive.hover.light: "#f3f4f6"
  colors.interactive.hover.dark: "#1f2937"
  colors.interactive.selected.light: "#e5e7eb"
  colors.interactive.selected.dark: "#374151"
  colors.interactive.accent.light: "#2563eb"
  colors.interactive.accent.dark: "#93c5fd"
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
rounded:
  sm: "0.25rem"
  md: "0.375rem"
  lg: "0.5rem"
  pill: "9999px"
spacing:
  tight: "0.75rem"
  compact: "1rem"
  comfortable: "1.25rem"
  section: "1.5rem"
  panel: "2rem"
components:
  button-primary:
    backgroundColor: "{colors.interactive.accent.light}"
    textColor: "{colors.surface.default.light}"
    rounded: "{rounded.lg}"
    padding: "0.5rem 0.875rem"
    height: "2.25rem"
  button-primary-hover:
    backgroundColor: "#1d4ed8"
    textColor: "{colors.surface.default.light}"
  button-quiet:
    backgroundColor: "{colors.interactive.hover.light}"
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
    backgroundColor: "{colors.interactive.selected.light}"
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

Token provenance for this spec comes from merged Panda output in `styled-system/tokens/index.mjs`, with semantic overrides in `stl-verify/ts/ui/panda.config.ts` and the shared `@archon-research/design-system/panda-preset`. Frontmatter color keys intentionally preserve semantic token paths and include explicit light and dark values.

**Key Characteristics:**
- Dense by design, readable at a glance.
- Neutral-first surfaces with one operational accent.
- Structured spacing rhythm from compact controls to roomy section breaks.
- Familiar controls and table affordances over experimental patterns.

## Colors

The palette is restrained and operational: cool neutrals carry most surfaces, blue accent is reserved for action and focus. Semantic tokens are the source of truth and are applied as light/dark pairs.

### Primary
- **colors.interactive.accent.light** (#2563eb): Primary actions, active selections, and focus-relevant emphasis.
- **colors.interactive.accent.dark** (#93c5fd): Accent equivalent in dark mode.

### Neutral
- **colors.surface.subtle.light / .dark** (#f9fafb / #111827): Application background in each theme.
- **colors.surface.default.light / .dark** (#ffffff / #030712): Main panel and control surface.
- **colors.surface.elevated.light / .dark** (#ffffff / #111827): Overlay and elevated panel surfaces.
- **colors.border.subtle.light / .dark** (#e5e7eb / #1f2937): Input and divider borders.
- **colors.border.default.light / .dark** (#d1d5db / #374151): Stronger state borders and selected-row outlines.
- **colors.text.strong.light / .dark** (#030712 / #f9fafb): Strong titles and key labels.
- **colors.text.default.light / .dark** (#374151 / #d1d5db): Core body and table text.
- **colors.text.muted.light / .dark** (#4b5563 / #9ca3af): Metadata and supporting copy.

### Named Rules
**The Signal Budget Rule.** Accent blue is used only when an element is actionable, selected, or needs immediate operator attention.

## Typography

**Display Font:** IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif  
**Body Font:** IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif  
**Label/Mono Font:** IBM Plex Sans, SF Pro Text, Segoe UI, sans-serif

**Character:** Utility-first and technically neutral. Typography supports quick parsing with minimal personality overhead.

### Hierarchy
- **Display** (400, 2.25rem, 1.25): Prime name and highest-level panel titles.
- **Headline** (600, 1.5rem, 1.25): Section-level emphasis and major subheads.
- **Title** (600, 1rem, 1.5): Metric titles and table-adjacent headings.
- **Body** (400, 0.875rem, 1.4): Primary content rows, control values, and standard copy.
- **Label** (600, 0.75rem, 0.04em tracking): Tabs, chips, and compact metadata.

### Named Rules
**The Operator Scan Rule.** For data-heavy zones, keep body copy in the 0.75rem to 0.875rem range and reserve larger type for wayfinding anchors only.

## Elevation

Depth is mostly tonal rather than shadow-driven. Most surfaces remain flat, with subtle border contrast and background shifts carrying hierarchy. Shadows appear only in contained overlays and selected-detail emphasis.

### Shadow Vocabulary
- **Overlay Lift** (`0 8px 20px rgba(15, 23, 42, 0.05)`): Lightweight lift for elevated table zones.
- **Panel Lift** (`0 24px 80px rgba(15, 23, 42, 0.08)`): Main dashboard section framing.
- **Focus Ring Inset** (`inset 0 0 0 1px #d1d5db`): Selected row emphasis without heavy glow.

### Named Rules
**The Flat-by-Default Rule.** Base states stay flat, elevation appears only to clarify layering or active context.

## Components

### Buttons
- **Shape:** Rounded rectangle with 0.5rem corners.
- **Primary:** Blue fill with white text, compact horizontal padding, optimized for control bars.
- **Hover / Focus:** Slight darkening of blue plus visible border/focus intent.
- **Quiet:** Gray-toned button style for low-priority actions and segmented controls.

### Chips
- **Style:** Pill geometry on muted gray background with uppercase tracking.
- **State:** Used to mark section context and compact taxonomy labels.

### Cards / Containers
- **Corner Style:** 0.375rem to 0.5rem based on hierarchy depth.
- **Background:** White for primary panels, gray-50/gray-100 for surrounding and secondary zones.
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
