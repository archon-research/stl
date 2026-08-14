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
  // of the 41 recipes the preset registers, the 37 the app never calls itself
  // are invisible to the scan of src/ and emit NOTHING -- every design-system
  // component built on them renders unstyled. For the four the app does call
  // (`toggleSwitch`, `segmentedControl`, `surfaceMessage`, `tooltip`) Panda
  // emits only the defaults it can infer, so any variant chosen from data still
  // lands on a class with no CSS behind it. All of it fails silently. staticCss
  // is a Panda root-config key, so the preset cannot carry this for us;
  // spreading the exported map is the supported fix.
  //
  // The spread is wider than this app needs, and narrowing it is now allowed:
  // doctor's gate asks only that *at least one* design-system recipe stem be
  // present, rather than hardcoding `drawer__content--size_lg` as required
  // regardless of consumer usage. Narrowing to the surfaces actually rendered
  // is a follow-up, not a blocked one.
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
          bg: {
            // Completes the preset's `bg.*` status family, which ships
            // canvas/success/critical/warning but no neutral fill. Upstream gap
            // tracked as ORB-352; delete this once the preset ships it.
            neutral: {
              value: {
                base: '{colors.neutral.100}',
                _dark: '{colors.neutral.800}',
              },
            },
          },
        },
      },
    },
  },
});
