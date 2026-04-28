import fs from 'node:fs';
import path from 'node:path';

import { z } from 'zod';

const envSchema = z.object({
  API_URL: z.url(),
  VITE_API_BASE_URL: z
    .string()
    .trim()
    .default('')
    .refine(
      (value) => value.length === 0 || z.url().safeParse(value).success,
      'VITE_API_BASE_URL must be an empty string or a valid URL',
    ),
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

function loadEnvWithDefaultBase(mode: string, envDir: string): Record<string, string> {
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
