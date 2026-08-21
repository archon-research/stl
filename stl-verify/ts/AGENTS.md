# stl-verify/ts

React UI, **frontend only** (npm workspaces). Go service conventions: [../AGENTS.md](../AGENTS.md). Root: [../../AGENTS.md](../../AGENTS.md).

Workspaces: `ui` (the app) and `mocks` (`@stl-verify/mocks`, typed offline API
mocks — read [mocks/README.md](mocks/README.md) before touching a fixture or
handler).

## Tooling & commands

- Hooks (lefthook): oxlint, oxfmt.
- CI (`ts-ci.yml`): `lint` + `format:check`, then panda codegen, `doctor`, the
  `test:*` regression scripts (incl. `test:mocks`), the openapi-types sync check,
  `type:check`, `build` — **source of truth**.
- Tools: `npm ci` (oxlint, oxfmt, etc.).
- On a fresh `npm ci`, run `npm run prepare -w ui` (panda codegen) before `npm run type:check`/`build`, else `#styled-system/*` imports fail.

```bash
cd stl-verify/ts
npm ci
npm run prepare -w ui                    # panda codegen (needed before type:check/build on fresh install)
npm run dev --workspace=@stl-verify/ui   # run the UI locally

VITE_API_MOCKS=1 npm run dev -w ui       # run it offline, no backend, no API_URL
npm run test:mocks -w mocks              # exercise the mocks through the app's API client
```

`API_URL` is required except under `VITE_API_MOCKS=1`; `env.ts` holds that rule.

Don't bypass hooks.
