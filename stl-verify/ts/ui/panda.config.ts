import { designSystemStaticCssRecipes } from '@archon-research/design-system';
import { designSystemPreset } from '@archon-research/design-system/panda-preset';
import { defineConfig } from '@pandacss/dev';

export default defineConfig({
  presets: [
    '@pandacss/preset-base',
    '@pandacss/preset-panda',
    designSystemPreset,
  ],
  preflight: true,
  include: ['./src/**/*.{ts,tsx,js,jsx}'],
  exclude: [],
  gitignore: true,
  outdir: 'styled-system',
  jsxFramework: 'react',
  // The design system's components build their recipe class names at runtime
  // (`badge--variant_${variant}`), so Panda's static scan of src/ cannot see
  // which variants are reachable and emits only the defaults it can infer.
  // Every variant chosen from data -- which is most of them here, e.g.
  // StatusBadge -- would then render with a class that has no CSS behind it,
  // silently. staticCss is a Panda root-config key, so the preset cannot carry
  // this for us; spreading the exported map is the supported fix.
  staticCss: {
    recipes: {
      ...designSystemStaticCssRecipes,
    },
  },
  theme: {
    extend: {
      semanticTokens: {
        colors: {
          // Only tokens the design-system preset does NOT provide. Every
          // surface/border/text/interactive token this app used to redefine is
          // gone: the preset now ships an equivalent-or-better value, and a
          // local copy silently reverts upstream token fixes -- most visibly
          // the dark elevation ramp, where the old `surface.default:
          // gray.950` made a "raised" panel the darkest thing on the page.
          text: {
            // A 4th step below `muted`. The preset's text ramp stops at
            // strong/default/muted, but 13 call sites want one step quieter
            // than `muted`. Pinned to the preset's `text.muted` value for now:
            // anything lighter than neutral.500 on white fails WCAG AA
            // (neutral.400 = 2.52:1), so a real 4th step has to be inserted
            // between `default` and `muted`, which is a design decision.
            subtle: {
              value: {
                base: '{colors.neutral.500}',
                _dark: '{colors.neutral.400}',
              },
            },
            // Theme-INVARIANT light text for always-dark fills
            // (`overlay.tooltip`). The preset's nearest token,
            // `colorPalette.solid.fg` on the neutral palette, flips across
            // themes by design -- wrong on a fixed dark overlay.
            inverse: {
              value: {
                base: '{colors.neutral.50}',
                _dark: '{colors.neutral.50}',
              },
            },
          },
          bg: {
            // Completes the preset's `bg.*` status family, which ships
            // success/critical/warning but no informational or neutral fill.
            info: {
              value: { base: '{colors.blue.50}', _dark: '{colors.blue.950}' },
            },
            neutral: {
              value: {
                base: '{colors.neutral.100}',
                _dark: '{colors.neutral.800}',
              },
            },
          },
          overlay: {
            // The preset has no `overlay.*` family at all. Scrims and
            // always-dark tooltip fills are semi-transparent, so they cannot
            // be expressed as a step on the opaque surface ramp.
            backdrop: {
              value: {
                base: 'rgb(15 23 42 / 0.28)',
                _dark: 'rgb(3 7 18 / 0.48)',
              },
            },
            tooltip: {
              value: {
                base: 'rgb(15 23 42 / 0.96)',
                _dark: 'rgb(3 7 18 / 0.96)',
              },
            },
          },
          interactive: {
            // Compatibility shim, NOT a design choice, and load-bearing: three
            // shipped components -- ErrorState.js:58, ErrorBoundary.js:63,
            // RangePicker.js:103 -- read `var(--colors-interactive-accent,
            // <hex>)` from an inline style, yet the preset defines no such
            // token. Without this definition the var is unresolved and all
            // three fall back to a hardcoded light-mode blue in BOTH themes
            // (and to two different blues, #2563eb vs #1d4ed8). Values are
            // kept identical to the preset's `text.interactive` so the app has
            // one accent, not two. Delete once upstream defines the token or
            // stops reading it.
            accent: {
              value: { base: '{colors.blue.600}', _dark: '{colors.blue.300}' },
            },
          },
          chart: {
            series: {
              // The preset's series scale runs out of ORDINAL roles at
              // `tertiary`; the remaining two (`positive`, `critical`) are
              // semantic and would mis-signal on a routine capital/debt
              // metric. The 4-up metric rail needs a 4th ordinal hue, so
              // extend the scale rather than borrow a status colour. These
              // preserve the rail's historical amber/orange identity but as
              // dark-aware palette steps instead of the raw #f59e0b/#f97316.
              // Filed upstream as a gap in the ordinal scale.
              quaternary: {
                value: {
                  base: '{colors.amber.600}',
                  _dark: '{colors.amber.300}',
                },
              },
              quinary: {
                value: {
                  base: '{colors.orange.600}',
                  _dark: '{colors.orange.300}',
                },
              },
            },
          },
        },
      },
    },
  },
});
