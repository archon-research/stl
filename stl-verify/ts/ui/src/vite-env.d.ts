/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL: string;
  // Vite substitutes the literal text of the env value, so main.tsx compares
  // against `'1'`.
  readonly VITE_API_MOCKS?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
