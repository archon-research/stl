# stl-verify/ts

React UI, **frontend only** (npm workspaces). Go service conventions: [../AGENTS.md](../AGENTS.md). Root: [../../AGENTS.md](../../AGENTS.md).

## Tooling & commands

- Hooks (lefthook): oxlint, oxfmt.
- CI (`ts-ci.yml`): `npm run lint` + `npm run format:check` + `npm run build` — **source of truth**.
- Tools: `npm ci` (oxlint, oxfmt, etc.).
- On a fresh `npm ci`, run `npm run prepare` (panda codegen) before `npm run type:check`/`build`, else `#styled-system/*` imports fail.

```bash
cd stl-verify/ts
npm ci
npm run prepare                          # panda codegen (needed before type:check/build on fresh install)
npm run dev --workspace=@stl-verify/ui   # run the UI locally
```

Don't bypass hooks.
