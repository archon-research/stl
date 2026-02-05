# Sentinel Data Linear Issues Proposal

## Infrastructure & Configuration

### Note on the Aggregation of Configuration
The following should all be high priority in order to be better coordinated, both for core contributors and external parties. It will also benefit transparency and auditing of Sentinel.

- [ ] Aggregation of indexing configuration
  - **Description:** For smart contracts: ABIs, addresses, versions, deployment dates. For tokens: addresses by chain, decimals, etc. Also, RPC nodes used, external APIs used, and more.
  - **Labels:** `documentation`
  - **Phase:** 1

- [ ] Aggregation of allocation tracking, and prices/oracles configurations
  - **Description:** Addresses.
  - **Labels:** `documentation`
  - **Phase:** 1

- [ ] Aggregation of RWA risk engine configuration
  - **Description:** Data sources.
  - **Labels:** `documentation`
  - **Phase:** 1

---

## Generic Infrastructure

- [ ] Complete worker infrastructure
  - **Labels:** `infrastructure`
  - **Phase:** 1

- [ ] Test indexing and worker infrastructure on all required chains
  - **Note:** also need alternative to Alchemy for Plume
  - **Labels:** `infrastructure`
  - **Phase:** 1

---

## Indexing Infrastructure

### Note on Meaning of "Indexing"
For Verify Phase 1, the specification is to calculate in realtime the CRR for current Star allocations only. This means, for lending, we:
- Only fetch realtime data for the Star-specific allocations.
- Only fetch the historical data for the Star-specific allocations, and only for the time period required for the CRR formula (possibly 30d, possibly longer, TBD).
For RWAs, we have more flexibility and will prioritize building a generic "calculator".
For lending, we will need to manually update the indexing infra and configuration as new allocations are made until Sentinel is in a more advanced Phase.

### Indexed Data
Lending protocols: supplied token balances, (historical) supplied token prices, (historical) APYs, market params, (historical) underlying token prices, underlying token breakdown, pool utilization, full borrower position snapshot.
DEXes: LP token balances, pool composition, pool token prices.
RWA tokenization platforms: Held token balance, token price.

### Index Star-Allocated Onchain Protocols

- [ ] Index Sparklend
  - **Labels:** `indexing`
  - **Note:** first Ethereum, then Gnosis
  - **Phase:** 1

- [ ] Index Aave
  - **Labels:** `indexing`
  - **Note:** first Ethereum, then Avalanche
  - **Phase:** 1

- [ ] Index Maple
  - **Labels:** `indexing`
  - **Phase:** 1

- [ ] Index Morpho
  - **Labels:** `indexing`
  - **Note:** first Ethereum, then Base
  - **Phase:** 3

- [ ] Index DEXes (Curve and Uniswap)
  - **Labels:** `indexing`
  - **Phase:** 2

- [ ] Index RWA tokenization platforms (Securitize, Centrifuge, etc.)
  - **Labels:** `indexing`
  - **Phase:** 2

---

## Data Collection & Processing

#### Historical State Gathering
- [ ] Create scripts for historical state gathering
  - **Labels:** `indexing`
  - **Phase:** 1

- [ ] Implement event-based position tracking for Aave (if full positions snapshot proves too difficult)
  - **Note:** May not be needed immediately, since we need the backing only for current allocations at first
  - **Labels:** `indexing`
  - **Phase:** 3

#### Price Data
- [ ] Extraction and storing of lending market specific prices
  - **Labels:** `indexing`
  - **Phase:** 1

- [ ] Implement generic (not specific to particular lending market) onchain price fetching
  - **Labels:** `indexing`
  - **Phase:** 1

- [ ] Implement fetching from external APIs for cases not covered by above/fallbacks
  - **Labels:** `indexing`
  - **Phase:** 1

#### (TBD if needed) Historical Liquidity & Volume Data
- [ ] Research and implement historical onchain liquidity collection
  - **Description:** May need to use variety of methods. DEX aggregators or CoinGecko could work for chain-wide current liquidity.
  - **Labels:** `indexing`, `research`
  - **Phase:** 2

- [ ] Research on Historical DEX indexing for historical liquidity
  - **Description:** Not feasible for full implementation, but would it help for doing case studies for developing the risk model?
  - **Labels:** `indexing`, `research`
  - **Phase:** 2

- [ ] Research and implement historical volume data collection
  - **Description:** Needs research, possibly merging of several sources required
  - **Labels:** `indexing`, `research`
  - **Phase:** 2

#### RWA Data Processing
- [ ] Implement ingestion, storing, and processing of RWA prices, volumes, liquidities from different sources
  - **Labels:** `indexing`
  - **Phase:** 1

---

## Star Allocation Tracking

- [ ] Implement tracking of Star allocations
  - **Labels:** `indexing`
  - **Phase:** 1
  - Track token balances
  - Handle exceptions (e.g., Galaxy)
  - Handle allocations in transition (not strictly necessary, but affects alerting)
  - Track RRC

---

## Python Business Layer

- [ ] Implement Python business layer
  - **Labels:** `infrastructure`
  - **Phase:** 2
  - For initial lending market risk engine, will need to:
  - Get `asset correlation`
  - Get `historical APYs`
  - Get `asset volatility`
  - Get `lending market params` (liquidation threshold, etc.)
  - Get `liquidation depth`
  - Get `backed breakdown`
  - Get `star allocation value`

---

## Risk Engines

- [ ] Add initial lending market risk engine (follow current BA methodology)
  - **Labels:** `infrastructure`
  - **Phase:** 1

- [ ] Implement updated lending market risk engine
  - **Labels:** `infrastructure`
  - **Phase:** 2

#### RWA Risk Engines

- [ ] Add initial RWA risk engine
  - **Labels:** `infrastructure`
  - **Phase:** 1

- [ ] Possible expansion of RWA risk engine to account for different classes of assets
  - **Labels:** `infrastructure`
  - **Phase:** 2

---

## API & Monitoring

#### API
- [ ] Define routes to interact with risk engines (FastAPI)
  - **Labels:** `infrastructure`
  - **Phase:** 2

#### Alerting & Monitoring
- [ ] Implement monitoring and alerting
  - **Labels:** `infrastructure`
  - **Phase:** 3
  - Price change/bounds monitoring
  - Balance change/bounds monitoring
  - Allocation movement monitoring
  - Indexing/RPC error monitoring
  - External API error monitoring
  - RRC alerting
  - Monitoring for inputs to RRC calculations (e.g., APYs) bounds
  - Volume and liquidity change/bounds monitoring

---

## Settle

- [ ] Implement allocation tracking
  - **Labels:** `indexing`
  - **Phase:** 1

- [ ] Implement idle PSM tracking
  - **Labels:** `indexing`
  - **Phase:** 4

- [ ] Implement idle ALM tracking
  - **Labels:** `indexing`
  - **Phase:** 4

- [ ] Implement ilk tracking (total debt)
  - **Labels:** `indexing`
  - **Phase:** 4

- [ ] Implement tracking of USDS balances across all wallets for tagging (all bridged forms)
  - **Labels:** `indexing`
  - **Phase:** 4

- [ ] Implement business logic for new Settle framework
  - **Description:** Idle assets within allocations, within PSM/ALM, accounting for Sky direct exposures. Possible untangling of Spark Liquidity Layer?
  - **Labels:** `infrastructure`
  - **Phase:** 4

- [ ] Implement application of custom rules
  - **Labels:** `infrastructure`
  - **Phase:** 4

- [ ] Implement handling of exceptions and discrepancies
  - **Labels:** `indexing`
  - **Phase:** 4

- [ ] Track everything not covered by above, to show Star solvency at a glance
  - **Labels:** `indexing`
  - **Phase:** 5

- [ ] Build APIs/dashboards for external consumers
  - **Labels:** `infrastructure`
  - **Phase:** 5

---

## Documentation

- [ ] Write documentation on all used allocation and indexing methodologies, once implemented
  - **Labels:** `documentation`
  - **Phase:** 3

- [ ] Gather feedback on indexing knowledge documentation
  - **Labels:** `documentation`
  - **Phase:** 1

- [ ] Prepare new Settle framework proposal
  - **Labels:** `documentation`, `research`
  - **Phase:** 1

- [ ] Prepare proposals for Atlas additions on reporting/tech requirements for new Star allocations
  - **Labels:** `documentation`
  - **Phase:** 3

- [ ] Write documentation on procedures for maintenance responsibilities, resolving disputes, etc.
  - **Labels:** `documentation`
  - **Phase:** 3
---
