import { defineConfig } from '@pandacss/dev';

export default defineConfig({
  preflight: true,
  include: ['./src/**/*.{ts,tsx,js,jsx}'],
  exclude: [],
  gitignore: true,
  outdir: 'styled-system',
  jsxFramework: 'react',
  theme: {
    extend: {
      semanticTokens: {
        colors: {
          surface: {
            default: {
              value: { base: '{colors.white}', _dark: '{colors.gray.950}' },
            },
            subtle: {
              value: { base: '{colors.gray.50}', _dark: '{colors.gray.900}' },
            },
            elevated: {
              value: { base: '{colors.white}', _dark: '{colors.gray.900}' },
            },
          },
          border: {
            subtle: {
              value: { base: '{colors.gray.200}', _dark: '{colors.gray.800}' },
            },
            default: {
              value: { base: '{colors.gray.300}', _dark: '{colors.gray.700}' },
            },
          },
          text: {
            muted: {
              value: { base: '{colors.gray.600}', _dark: '{colors.gray.400}' },
            },
            default: {
              value: { base: '{colors.gray.700}', _dark: '{colors.gray.300}' },
            },
            strong: {
              value: { base: '{colors.gray.950}', _dark: '{colors.gray.50}' },
            },
          },
          interactive: {
            hover: {
              value: { base: '{colors.gray.100}', _dark: '{colors.gray.800}' },
            },
            selected: {
              value: { base: '{colors.gray.200}', _dark: '{colors.gray.700}' },
            },
            accent: {
              value: { base: '{colors.blue.600}', _dark: '{colors.blue.300}' },
            },
          },
        },
      },
    },
  },
});
