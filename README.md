# Multi-Protocol Lending Indexer

A modular Ponder-based blockchain indexer for Aave V3-like lending protocols. Currently indexes:
- **Sparklend** (Mainnet & Gnosis)
- **Aave V3** (Core & Horizon on Mainnet)

## Architecture

This indexer is designed for scalability and modularity:

1. **Protocol Isolation** - Each protocol has its own configuration, ABIs, handlers, and schemas
2. **Shared Schemas** - Common entities (Chain, Protocol, Token, User) normalized across protocols
3. **Aave V3 Architecture** - Protocols share schema structures (AToken, ReserveConfig, UserPositionBreakdown)
4. **Multi-Chain Support** - Single codebase indexes Mainnet and Gnosis Chain
5. **Event-Driven** - Real-time indexing with optional historical snapshot jobs

## Project Structure

```
├── abis/                                    # Contract ABIs by protocol
│   ├── sparklend/
│   └── aave/
├── src/
│   ├── schema/                              # Shared schema definitions
│   │   ├── common/                          # Universal (Chain, Protocol, Token, User)
│   │   └── aave-v3/                         # Aave V3 architecture (AToken, ReserveConfig)
│   ├── protocols/                           # Protocol implementations
│   │   ├── sparklend/
│   │   │   ├── config/                      # Contract configurations
│   │   │   ├── schema/                      # Chain-specific schemas
│   │   │   ├── handlers/                    # Event handlers
│   │   │   ├── jobs/                        # CLI jobs
│   │   │   └── utils/                       # Utilities
│   │   └── aave/
│   ├── scripts/                             # Job runners
│   ├── db/                                  # Database helpers
│   └── api/                                 # REST API
├── ponder.config.ts                         # Main configuration
└── ponder.schema.ts                         # Schema exports
```

## Schema Design

### Shared Schemas (`src/schema/common/`)

Universal reference tables:
- **Chain** - Blockchain networks (Mainnet, Gnosis)
- **Protocol** - Protocol instances (sparklend-mainnet, aave-core-mainnet)
- **Token** - Underlying assets (WETH, USDC, DAI)
- **User** - User addresses

### Aave V3 Architecture (`src/schema/aave-v3/`)

Shared patterns for Aave V3-like protocols:
- **AToken** - Protocol-specific yield tokens
- **ReserveConfig** - Reserve configuration snapshots
- **UserPositionBreakdown** - Position analytics

### Protocol-Specific Schemas

Each protocol/chain gets its own tables:
- `SparklendMainnetBorrow`, `SparklendMainnetSupply`, etc.
- `AaveMainnetCoreBorrow`, `AaveMainnetCoreSupply`, etc.

## Indexed Protocols

### Sparklend

**Networks**: Ethereum Mainnet, Gnosis Chain

**Contracts**:
- Mainnet: `0xC13e21B648A5Ee794902342038FF3aDAB66BE987` (block 16776401)
- Gnosis: `0x2Dae5307c5E3FD1CF5A72Cb6F698f915860607e0` (block 29817457)

### Aave V3

**Networks**: Ethereum Mainnet (Core & Horizon)

**Contracts**:
- Core: `0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2` (block 16291127)
- Horizon: `0x1E4f2Ab406aa9764ff05a9a8c8b6F531f` (block TBD)

## Features

### Real-Time Event Indexing

- Supply/Withdraw/Borrow/Repay transactions
- Liquidations and flash loans
- Collateral toggles
- Reserve updates
- Token transfers

### Snapshot Jobs (CLI)

Optional historical snapshot calculations:

```bash
# Capture asset prices
pnpm job price-capture mainnet 16776401 17000000 7200

# Calculate health factors
pnpm job snapshot-calculation mainnet 16776401 17000000 7200
```

See [`src/scripts/README.md`](./src/scripts/README.md) for details.

## API

### GraphQL

Access at `http://localhost:42069/graphql`

```graphql
query {
  sparklendMainnetBorrows(orderBy: "timestamp", orderDirection: "desc", limit: 10) {
    items {
      user
      amount
      borrowRate
      timestamp
    }
  }
}
```

### REST

```bash
GET /events/:eventName?limit=10

# Examples
curl http://localhost:42069/events/SparklendMainnetBorrow
curl http://localhost:42069/events/AaveMainnetCoreLiquidationCall
```

## Setup

### Prerequisites

- Node.js >= 18.14
- pnpm >= 9.5.0
- Alchemy API key

### Installation

```bash
pnpm install
```

### Environment

Create `.env.local`:

```env
PONDER_RPC_URL_1=https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY
PONDER_RPC_URL_100=https://rpc.gnosischain.com
```

### Development

```bash
# Start dev server
pnpm dev

# Type check
pnpm typecheck

# Lint
pnpm lint
```

### Docker

```bash
# Start with Docker Compose
pnpm docker:up

# View logs
pnpm docker:logs

# Stop
pnpm docker:down
```

## Database

- **Local**: PGlite (automatic, no setup)
- **Production**: PostgreSQL via `DATABASE_URL`

## Deployment

See [DEPLOYMENT.md](./DEPLOYMENT.md) for Railway deployment instructions.

**Quick deploy:**
1. Push to GitHub
2. Create Railway project
3. Add PostgreSQL
4. Set `PONDER_RPC_URL_1` and `PONDER_RPC_URL_100`
5. Deploy

## Adding Protocols

1. Create `src/protocols/[name]/`
2. Add ABIs to `abis/[name]/`
3. Create config, schemas, and handlers
4. Register in `ponder.config.ts` and `src/index.ts`

## Built With

- [Ponder](https://ponder.sh) - Blockchain indexing
- [Viem](https://viem.sh) - Ethereum library
- [Drizzle ORM](https://orm.drizzle.team/) - TypeScript ORM
- [PostgreSQL](https://www.postgresql.org/) - Database

## License

Private and proprietary.
