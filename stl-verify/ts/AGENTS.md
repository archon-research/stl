# stl-verify/ts

React UI, **frontend only** (npm workspaces). Go service conventions: [../AGENTS.md](../AGENTS.md). Root: [../../AGENTS.md](../../AGENTS.md).

Workspaces: `ui` (the app) and `mocks` (`@stl-verify/mocks`, typed offline API
mocks — read [mocks/README.md](mocks/README.md) before touching a fixture or
handler).

## Tooling & commands

- Hooks (lefthook): oxlint, oxfmt — both invoked through `uikit-cli`, never the
  oxlint/oxfmt binaries by name (they are transitive deps of `uikit-cli`, not
  declared ones).
- CI (`ts-ci.yml`): `lint` + `format:check`, then panda codegen, `doctor`, the
  `test:*` regression scripts (incl. `test:mocks`), the openapi-types sync check,
  `type:check`, `build` — **source of truth**.
- Tools: `npm ci` (oxlint, oxfmt, etc.).
- `lint` runs oxlint with `--type-aware` (needs `oxlint-tsgolint`),
  `--max-warnings=0` (oxlint exits 0 on warnings, and the design-system boundary
  rule is a warning) and `--report-unused-disable-directives`. The pre-commit
  hook deliberately omits `--type-aware`: it costs a whole TS program load, and
  CI is where it belongs.
- Every gate covers `scripts/` too. The `test:*` scripts are **`.ts`, run
  directly by Node** (24 strips types natively), and they are linted, formatted
  and type-checked like the rest — `ui/tsconfig.node.json` and
  `mocks/tsconfig.json` include them. Keep them erasable-syntax only: no enums,
  no parameter properties (`erasableSyntaxOnly` is on).
- `@types/node` tracks the major in `.node-version`; bumping one without the
  other type-checks the node-side code against a runtime nobody runs.
- On a fresh `npm ci`, run `npm run prepare -w ui` (panda codegen) before `npm run type:check`/`build`, else `#styled-system/*` imports fail.
- Node ships with an older npm than `engines.npm` requires; run `corepack enable npm` once so npm resolves to the pinned version (`packageManager`).

```bash
cd stl-verify/ts
corepack enable npm
npm ci
npm run prepare -w ui                    # panda codegen (needed before type:check/build on fresh install)
npm run dev --workspace=@stl-verify/ui   # run the UI locally

VITE_API_MOCKS=1 npm run dev -w ui       # run it offline, no backend, no API_URL
npm run test:mocks -w mocks              # exercise the mocks through the app's API client
```

`API_URL` is required except under `VITE_API_MOCKS=1`; `env.ts` holds that rule.

Don't bypass hooks.
