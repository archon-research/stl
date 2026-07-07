# stl-verify/ts

React UI, **frontend only** (npm workspaces). Go service conventions: [../CLAUDE.md](../CLAUDE.md). Root: [../../CLAUDE.md](../../CLAUDE.md).

## Tooling & commands

- Hooks (lefthook): oxlint, oxfmt.
- CI (`ts-ci.yml`): `npm run lint` + `npm run format:check` + `npm run build` — **source of truth**.
- Tools: `npm install` (oxlint, oxfmt, etc.).

```bash
cd stl-verify/ts
npm install
npm run dev --workspace=@stl-verify/ui   # run the UI locally
```

Don't bypass hooks.
