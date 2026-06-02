# PRODUCT: STL Verify

## Product Purpose
STL Verify is an operational dashboard for monitoring prime allocation exposure, risk capital, and activity freshness across protocols and chains. The interface should optimize for rapid scanning, confident filtering, and low-friction drill-down during routine monitoring and incident triage.

## Users
Primary users are risk operators, protocol analysts, and engineers reviewing allocation health and debt exposure. They use the app in data-dense sessions where clarity, consistency, and speed matter more than decorative novelty.

## Top Workflows
1. Scan prime allocation health: select a prime in the sidebar, read the summary metric rail, then scan the allocation grid.
2. Filter and search: narrow allocations by network and protocol, or search assets, protocols, and chains, with state preserved in the URL.
3. Drill into risk: open the risk detail drawer for an allocation to inspect risk breakdown and required risk capital.
4. Triage freshness and activity: use the bottom panel tabs (risk breakdown, required risk capital, activity) and status and freshness signals to spot stale or anomalous data.

## Brand / Voice
Trustworthy, direct, technically precise, and calm under pressure. UI copy is concise and unambiguous, states facts over adjectives, and never overstates confidence in data that is stale or partial. Surface uncertainty (loading, stale, error) explicitly rather than hiding it.

## Strategic Principles
- Prioritize scanability and predictable layout over flourish.
- Keep dense information readable at a glance.
- Preserve stable interaction patterns across views.
- Surface state changes and errors clearly.
- Make responsive behavior structural, not cosmetic.

## Anti-references (what this UI should not become)
- Not a marketing dashboard: no hero gradients, glassmorphism, neon accents, or decorative motion competing with risk signals.
- Not a consumer fintech app: no gamified flourishes, celebratory animation, or novelty controls replacing familiar table and filter affordances.
- Not a BI tool dump: no wall of undifferentiated charts; evidence (the table) stays primary and metrics support it.
- No ambiguous status: never present stale or partial data as authoritative.

## Quality Bar (definition of done)
- Token discipline: color, surface, border, and text styling resolves through semantic Panda tokens; no hardcoded hex bypassing tokens in shipped components.
- Consistency: control heights stay around 2.25rem across selects, inputs, and quiet buttons; spacing follows the documented rhythm.
- Theming: full light and dark parity, verified in both themes.
- Accessibility basics: visible focus on all interactive elements, keyboard reachability for sidebar, filters, table, and drawer, and labels on controls.
- State coverage: loading, empty, error, and stale states are designed and reachable, not afterthoughts.
- Scan performance: the primary dashboard stays readable and responsive at representative allocation counts without layout thrash.

## Last refreshed from
- App structure: `stl-verify/ts/ui/src/App.tsx` and `src/components/` (PrimeSidebar, TopBar, SummaryMetric, AllocationGrid, BottomPanel, RiskDetailDrawer).
- Design tokens: `stl-verify/ts/ui/panda.config.ts` and `DESIGN.md`.
