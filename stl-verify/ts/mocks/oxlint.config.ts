import baseConfig from '@archon-research/oxlint-config/base';

const config = {
  ...baseConfig,
  categories: {
    correctness: 'error',
    suspicious: 'error',
  },
  rules: {
    'no-console': 'error',
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
