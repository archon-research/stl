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
  // Narrowing the spread is allowed since 0.9.0: doctor's gate asks only that
  // *at least one* design-system recipe stem be present. So this lists only the
  // recipes this app can actually render, keyed off the upstream map so the
  // variant lists still come from the package.
  //
  // Rule for editing this list: importing a design-system component means
  // adding the recipe(s) it applies. An entry that is present but unused only
  // costs CSS bytes; an entry that is MISSING for a rendered component drops
  // that component's CSS entirely, and nothing fails — doctor only checks that
  // the map is non-empty, not that it is complete. So when in doubt, include.
  // Verify by building and grepping `dist/assets/*.css` for the component's
  // class stems (`recipe__slot` / `recipe--variant_value`), which the built JS
  // contains verbatim.
  staticCss: {
    recipes: {
      badge: designSystemStaticCssRecipes.badge,
      dataTable: designSystemStaticCssRecipes.dataTable,
      emptyState: designSystemStaticCssRecipes.emptyState,
      pageShell: designSystemStaticCssRecipes.pageShell,
      panel: designSystemStaticCssRecipes.panel,
      // Column-filter popover inside DataTable.
      popover: designSystemStaticCssRecipes.popover,
      searchInput: designSystemStaticCssRecipes.searchInput,
      segmentedControl: designSystemStaticCssRecipes.segmentedControl,
      // StyledSelect, plus DataTable's faceted filter and RangePicker.
      select: designSystemStaticCssRecipes.select,
      sidebarLayout: designSystemStaticCssRecipes.sidebarLayout,
      statTile: designSystemStaticCssRecipes.statTile,
      surfaceMessage: designSystemStaticCssRecipes.surfaceMessage,
      themeToggle: designSystemStaticCssRecipes.themeToggle,
      toggleSwitch: designSystemStaticCssRecipes.toggleSwitch,
      tooltip: designSystemStaticCssRecipes.tooltip,
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
