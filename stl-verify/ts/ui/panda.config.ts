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
  // Panda only emits CSS for recipe usage it can read in source text, and the
  // design system's components apply their recipes as hardcoded class-name
  // strings (`badge--variant_${variant}`) rather than calling the recipe fn. So
  // of the 24 recipes the preset registers, the 21 the app never calls itself
  // are invisible to the scan of src/ and emit NOTHING -- every design-system
  // component built on them renders unstyled. For the three the app does call
  // (`toggleSwitch`, `segmentedControl`, `surfaceMessage`) Panda emits only the
  // defaults it can infer, so any variant chosen from data still lands on a
  // class with no CSS behind it. All of it fails silently. staticCss is a Panda
  // root-config key, so the preset cannot carry this for us; spreading the
  // exported map is the supported fix.
  staticCss: {
    recipes: {
      ...designSystemStaticCssRecipes,
    },
  },
  theme: {
    extend: {
      semanticTokens: {
        colors: {
          // Only tokens the design-system preset does NOT provide. Never shadow
          // a preset token here: `theme.extend` merges last and wins, so a local
          // copy silently reverts upstream token fixes.
          text: {
            // A step quieter than `muted`, which is where the preset's text ramp
            // stops. Pinned to `text.muted`'s value: a genuinely lighter step
            // fails WCAG AA on white, so the real fix is a design decision (see
            // DESIGN.md, "Borders and text").
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
            // Foreground for `bg.violet`; see that token for why the palette's
            // own role tokens are unavailable.
            violet: {
              value: {
                base: '{colors.violet.700}',
                _dark: '{colors.violet.300}',
              },
            },
          },
          bg: {
            // Completes the preset's `bg.*` status family, which ships
            // success/critical/warning but no neutral fill.
            neutral: {
              value: {
                base: '{colors.neutral.100}',
                _dark: '{colors.neutral.800}',
              },
            },
            // A categorical fill carrying no status meaning. The preset's
            // role-based `colorPalette` tokens (`subtle.bg`/`subtle.fg`) exist for
            // only six palettes -- neutral, gray, green, red, amber, blue -- while
            // `ColorPalette` accepts every raw hue, so `colorPalette: 'violet'`
            // type-checks and then emits no `subtle.bg` at all. Of the six, gray is
            // indistinguishable from neutral and red reads as an alarm, so a
            // five-way status-free taxonomy cannot be expressed through
            // `colorPalette` alone. Filed upstream.
            violet: {
              value: {
                base: '{colors.violet.50}',
                _dark: '{colors.violet.950}',
              },
            },
          },
          overlay: {
            // Semi-transparent, so neither can be expressed as a step on the
            // opaque surface ramp; the preset ships no `overlay.*` family.
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
            // Load-bearing on two independent counts, so this token stays
            // defined whatever upstream does. App code reaches for it directly as
            // the accent; separately, three shipped design-system components
            // (ErrorState, ErrorBoundary, RangePicker) read
            // `var(--colors-interactive-accent, <hex>)` from an inline style
            // while the preset defines no such token, so leaving it undefined
            // makes those three fall back to a hardcoded light-mode blue in BOTH
            // themes -- and to two different blues, since the inline fallbacks
            // disagree. Values are kept identical to the preset's
            // `text.interactive` so the app has one accent, not two.
            accent: {
              value: { base: '{colors.blue.600}', _dark: '{colors.blue.300}' },
            },
          },
          chart: {
            series: {
              // The preset's series scale runs out of ORDINAL roles at
              // `tertiary`, and the remaining two (`positive`, `critical`) are
              // semantic -- they would mis-signal on a routine capital/debt
              // metric. Extending the ordinal scale is the only way to get a 4th
              // and 5th hue without borrowing a status colour. Filed upstream;
              // see DESIGN.md, "Chart series".
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
