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
    'import/max-dependencies': 'error',

    'typescript/no-floating-promises': 'error',
    'typescript/no-misused-promises': 'error',
    'typescript/await-thenable': 'error',
    'typescript/no-base-to-string': 'error',
    'typescript/no-unsafe-type-assertion': 'error',

    'typescript/consistent-return': 'off',
    'typescript/no-unnecessary-type-assertion': 'off',
    'typescript/no-unnecessary-type-parameters': 'off',
  },
  overrides: [
    {
      files: ['scripts/**'],
      rules: {
        'no-console': 'off',
        'typescript/no-unsafe-type-assertion': 'off',
      },
    },
  ],
};

export default config;
