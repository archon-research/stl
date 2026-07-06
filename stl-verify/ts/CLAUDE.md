# stl-verify/ts

TypeScript sub-service (UI). Go service conventions: [../CLAUDE.md](../CLAUDE.md). Root: [../../CLAUDE.md](../../CLAUDE.md).

## Linting & tooling

- Hooks (lefthook): oxlint, oxfmt
- CI (`ts-ci.yml`): `npm run lint` + `npm run format:check` + `npm run build` — **source of truth**
- Tools: `npm ci` (oxlint, oxfmt, etc.)
- Don't bypass hooks.
