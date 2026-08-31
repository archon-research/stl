import baseConfig from '@archon-research/oxlint-config/base';

const config = {
  ...baseConfig,
  categories: {
    correctness: 'error',
    suspicious: 'error',
  },
  rules: {
    // Spread rather than replaced: `base` ships `rules: {}` so that a preset
    // rule added later is not silently dropped by the overrides below.
    ...baseConfig.rules,
    'no-console': 'error',

    // Type-aware rules (`--type-aware` in the lint script). Kept in step with
    // ui/oxlint.config.ts so a floating promise is caught on either side of the
    // workspace; see that file for why the style rules below are off.
    'typescript/no-floating-promises': 'error',
    'typescript/no-misused-promises': 'error',
    'typescript/await-thenable': 'error',
    'typescript/no-base-to-string': 'error',
    'typescript/no-unsafe-type-assertion': 'off',
    'typescript/consistent-return': 'off',
    'typescript/no-unnecessary-type-assertion': 'off',
    'typescript/no-unnecessary-type-parameters': 'off',
  },
  overrides: [
    {
      // The self-test is a CLI: its report goes to stdout.
      files: ['scripts/**'],
      rules: {
        'no-console': 'off',
      },
    },
  ],
};

export default config;
