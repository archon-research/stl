---
description: Go-specific conventions for stl-verify — interfaces, errors, testing, comments, build
paths:
  - stl-verify/**/*.go
---

# Go conventions

Go-only rules for the stl-verify service. Language-agnostic conventions (testing
philosophy, function composition, comments, dedup, registries) live in the "Code
Conventions" section of [../../stl-verify/CLAUDE.md](../../stl-verify/CLAUDE.md).

- **Interfaces**: Behavior interfaces use the `-er` suffix (Reader, Publisher, BlockSubscriber). Ports follow the established noun patterns instead: persistence ports are `XxxRepository`, external-system ports are `XxxClient`/`XxxCache`/`XxxProvider`. Do not rename Repository/Client ports to `-er` forms.
- **Constructors**: Use `New` prefix
- **Amounts**: Wei / token amounts are `big.Int`, never `float64`.
- **Errors**:
    - Wrap with context: `fmt.Errorf("doing X: %w", err)`.
    - Never ignore errors.
    - Lean towards returning errors instead of continuing, unless there is an extremely good reason to continue instead.
    - **Fail hard and early on unexpected errors.**
    - **Never swallow a failure into partial success.** A sub-result that fails (a multicall sub-call, a batch row, one item in a loop) must propagate and stop the whole unit of work; do not default it to nil/zero/empty and keep going. Silent partial data is the worst outcome: it looks healthy, and repairing the holes later forces a backfiller rerun.
    - **A partial failure stops the whole event/block.** Do not ack, commit, or persist a partially-processed event. Stopping and retrying is correct; continuing with a hole is not.
    - **Poison pills get fixed or explicitly discarded, never silently skipped.** When an event persistently fails, the only acceptable responses are to make the code handle it, or to make a deliberate, explicit decision to discard that specific event. Silently dropping or defaulting it is forbidden.
    - **"Best effort" / `AllowFailure` reads still bubble up.** A call you issue is expected to succeed, so treat a failed result as an error and propagate it. If a value is genuinely optional for some inputs (e.g. a getter that does not exist on a particular contract/pool variant), do not issue the call for those inputs; gate it structurally. A NULL or absent value must be a documented structural fact, never the residue of a swallowed failure.
    - Panic only in `main`/`cmd` entry points. Everywhere else (`internal/`, adapters, services, libraries) return an error and let the caller deal with it, bubbling it up until it reaches `main`.
- **Testing**:
    - Prefer table-driven tests (each case under `t.Run`).
    - `main.go` entry points should also have 100% coverage. Move the `main.go` body into a `run(ctx, args) error` function and call only that from `main()` so you can test it.
    - For `main.go` files, only create integration tests.
- **Comments**:
    - The "standard-library behavior" the reader already knows includes Go zero values, nil-map reads, `json.Unmarshal` of null, `defer` order, etc. — don't comment them.
    - Keep package and exported-API doc comments, but make each say something the signature doesn't.
- **Function composition**: a function-length / complexity linter (golangci-lint `funlen`/`gocognit`) is the planned deterministic backstop so an over-long function fails CI automatically rather than relying on a reviewer noticing.
- **Binaries/Building**: When building binaries using `go build`, output to `stl-verify/dist`
- **Code structure**: In main.go files, keep main() at the top of the file.
