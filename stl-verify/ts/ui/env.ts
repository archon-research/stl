import fs from 'node:fs';
import path from 'node:path';

import { z } from 'zod';

// Treat an exported-but-empty value as absent. This also blanks a value a .env
// file set, since process.env wins in loadEnvWithDefaultBase below.
//
// Takes the inner schema rather than exposing a prefix to .pipe(), so the
// second .optional() cannot be forgotten.
const optionalEnv = <T extends z.ZodType<unknown, string>>(inner: T) =>
  z
    .string()
    .trim()
    .transform((value) => (value === '' ? undefined : value))
    .optional()
    .pipe(inner.optional());

const envSchema = z
  .object({
    // Optional here, required by the refine below unless mocking: with mocks on
    // there is no proxy target, so demanding a URL would be noise.
    API_URL: optionalEnv(z.url()),
    // Declared so a typo'd value fails at config time instead of silently
    // leaving mocks off and the app pointed at nothing.
    VITE_API_MOCKS: optionalEnv(z.literal('1')),
    VITE_API_BASE_URL: z
      .string()
      .trim()
      .default('')
      .refine(
        (value) => value.length === 0 || z.url().safeParse(value).success,
        'VITE_API_BASE_URL must be an empty string or a valid URL',
      ),
    // Dev-server overrides, for serving the UI on a fixed port behind a reverse
    // proxy. The only producer is external local-dev tooling.
    //
    // Not z.coerce.number(): that is Number(), so a typo'd '0x1389' would pass
    // the range check below as port 5001, and '1e4' as 10000.
    VITE_PORT: optionalEnv(
      z
        .string()
        .regex(/^\d+$/u, 'VITE_PORT must be a decimal port number')
        .transform(Number)
        .pipe(z.number().int().min(1).max(65535)),
    ),
    VITE_STRICT_PORT: optionalEnv(z.stringbool()),
    // A host to bind, never a boolean: `host: true` is reachable as 0.0.0.0 and
    // `host: false` is indistinguishable from unset, so booleans add nothing.
    // The refused tokens are the ones a boolean schema would have claimed -- they
    // read as on/off but Vite passes them through to the OS as hostnames, and '0'
    // resolves to 0.0.0.0, exposing the dev server on every interface.
    VITE_HOST: optionalEnv(
      z
        .string()
        .refine(
          (host) =>
            !/^(?:true|false|on|off|yes|no|y|n|enabled|disabled|\d+)$/iu.test(
              host,
            ),
          'VITE_HOST must be a hostname or IP address (0.0.0.0 for all interfaces)',
        ),
    ),
    // Deliberately cannot express Vite's `allowedHosts: true`, which disables the
    // rejection of unrecognised Host headers (DNS-rebinding protection). A value
    // that is present but names no host is a mistake rather than a request for
    // the default, so it fails instead of being discarded.
    VITE_ALLOWED_HOSTS: optionalEnv(
      z
        .string()
        .transform((value) =>
          value
            .split(',')
            .map((host) => host.trim())
            .filter((host) => host.length > 0),
        )
        .refine(
          (hosts) => hosts.length > 0,
          'VITE_ALLOWED_HOSTS was set but contained no hostnames',
        ),
    ),
  })
  .refine((env) => env.VITE_API_MOCKS === '1' || env.API_URL !== undefined, {
    path: ['API_URL'],
    error: 'API_URL is required unless VITE_API_MOCKS=1',
  });

export type AppEnv = z.infer<typeof envSchema>;

function parseEnvFile(filePath: string): Record<string, string> {
  if (!fs.existsSync(filePath)) {
    return {};
  }

  const content = fs.readFileSync(filePath, 'utf8');
  const parsedEntries: Record<string, string> = {};

  for (const rawLine of content.split(/\r?\n/u)) {
    const line = rawLine.trim();

    if (!line || line.startsWith('#')) {
      continue;
    }

    const equalsIndex = line.indexOf('=');

    if (equalsIndex <= 0) {
      continue;
    }

    const key = line.slice(0, equalsIndex).trim();
    let value = line.slice(equalsIndex + 1).trim();

    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    parsedEntries[key] = value;
  }

  return parsedEntries;
}

function loadEnvWithDefaultBase(
  mode: string,
  envDir: string,
): Record<string, string> {
  const modeName = mode.trim();
  const envFileNames = [
    '.env.default',
    '.env.local',
    `.env.${modeName}`,
    `.env.${modeName}.local`,
  ];

  const mergedEnv: Record<string, string> = {};

  for (const fileName of envFileNames) {
    const filePath = path.resolve(envDir, fileName);
    const loaded = parseEnvFile(filePath);

    Object.assign(mergedEnv, loaded);
  }

  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      mergedEnv[key] = value;
    }
  }

  return mergedEnv;
}

export function resolveAppEnv(mode: string, envDir: string): AppEnv {
  return envSchema.parse(loadEnvWithDefaultBase(mode, envDir));
}
