# Price Sources

## Onchain Oracles

### Chainlink (Ethereum, `chainlink_feed`)

| Asset  | Feed Address                                 | Quote | Decimals |
|--------|----------------------------------------------|-------|----------|
| WETH   | `0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419` | USD   | 8        |
| WBTC   | `0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c` | USD   | 8        |
| DAI    | `0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9` | USD   | 8        |
| USDC   | `0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6` | USD   | 8        |
| USDT   | `0x3E7d1eAB13ad0104d2750B8863b489D65364e32D` | USD   | 8        |
| cbBTC  | `0x2665701293fCbEB223D11A08D826563EDcCE423A` | USD   | 8        |
| PYUSD  | `0x8f1dF6D7F2db73eECE86a18b4381F4707b918FB1` | USD   | 8        |
| tBTC   | `0x8350b7De6a6a2C1368E7D4Bd968190e13E354297` | USD   | 8        |
| sDAI   | `0x29081f7aB5a644716EfcDC10D5c926c5fEE9F72B` | USD   | 8        |
| USDS   | `0xfF30586cD0F29eD462364C7e81375FC0C71219b1` | USD   | 8        |
| weETH  | `0x5c9C449BbC9a6075A2c061dF312a35fd1E05fF22` | ETH   | 18       |
| rETH   | `0x536218f9E9Eb48863970252233c8F271f554C2d0` | ETH   | 18       |
| wstETH | `0x8B6851156023f4f5A66F68BEA80851c3D905Ac93` | USD   | 8        |
| ezETH  | `0x636A000262F6aA9e1F094ABF0aD8f645C44f641C` | ETH   | 18       |
| rsETH  | `0x03c68933f7a3F76875C0bc670a58e69294cDFD01` | ETH   | 18       |
| LBTC   | `0x5c29868C58b6e15e2b962943278969Ab6a7D3212` | BTC   | 8        |

### Chronicle (Ethereum, `chronicle`)

Uses DirectCaller instead of Multicall3 (toll/whitelist requirements). All feeds return 18 decimals.

| Asset  | Feed Address                                 | Quote | Decimals |
|--------|----------------------------------------------|-------|----------|
| WETH   | `0xb074EEE1F1e66650DA49A4d96e255c8337A272a9` | USD   | 18       |
| wstETH | `0xA770582353b573CbfdCC948751750EeB3Ccf23CF` | USD   | 18       |
| USDS   | `0x74661a9ea74fD04975c6eBc6B155Abf8f885636c` | USD   | 18       |
| sUSDS  | `0x496470F4835186bF118545Bd76889F123D608E84` | USD   | 18       |
| USDC   | `0xCe701340261a3dc3541C5f8A6d2bE689381C8fCC` | USD   | 18       |
| USDT   | `0x7084a627a22b2de99E18733DC5aAF40993FA405C` | USD   | 18       |
| WBTC   | `0x286204401e0C1E63043E95a8DE93236B735d4BF2` | USD   | 18       |

### Redstone (Ethereum, `redstone`)

Uses Chainlink AggregatorV3 ABI. `roundId` is always 1 (no historical rounds).

| Asset  | Feed Address                                 | Quote | Decimals |
|--------|----------------------------------------------|-------|----------|
| weETH  | `0xdDb6F90fFb4d3257dd666b69178e5B3c5Bf41136` | USD   | 8        |
| WETH   | `0x67F6838e58859d612E4ddF04dA396d6DABB66Dc4` | USD   | 8        |
| WBTC   | `0xAB7f623fb2F6fea6601D4350FA0E2290663C28Fc` | USD   | 8        |
| wstETH | `0xe4aE88743c3834d0c492eAbC47384c84BcADC6a6` | USD   | 8        |
| PYUSD  | `0xcE18A2Bf89Fa3c56aF5Bde8A41efF967a6d63d26` | USD   | 8        |
| USDC   | `0xeeF31c7d9F2E82e8A497b140cc60cc082Be4b94e` | USD   | 8        |
| USDT   | `0x02E1F8d15762047b7a87BA0E5d94B9a0c5b54Ed2` | USD   | 8        |
| rsETH  | `0xA736eAe8805dDeFFba40cAB8c99bCB309dEaBd9B` | ETH   | 8        |
| ezETH  | `0xF4a3e183F59D2599ee3DF213ff78b1B3b1923696` | ETH   | 8        |
| LBTC   | `0xb415eAA355D8440ac7eCB602D3fb67ccC1f0bc81` | BTC   | 8        |

### Protocol-Native Oracles

#### SparkLend (Ethereum)

- **Oracle Address**: `0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD9`
- **Deployment Block**: 16,664,447
- **Price Decimals**: 8
- **Resolver**: PoolAddressesProvider at `0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e`

Prices all SparkLend reserve assets via `getPriceOracle()`.

#### Aave V3 (Ethereum)

- **Oracle Address**: `0x54586be62e3c3580375ae3723c145253060ca0c2`
- **Deployment Block**: 16,291,127
- **Price Decimals**: 8
- **Resolver**: PoolAddressesProvider at `0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e`

Prices all 42 Aave V3 reserve assets.

#### Aave V3 (Avalanche)

- **Oracle Address**: `0xEBd36016B3eD09D4693Ed4251c67Bd858c3c7C9C`
- **Deployment Block**: 11,970,506
- **Price Decimals**: 8

### Quote Currency Resolution

Non-USD feeds are converted to USD using reference feeds:

| Quote | Reference Asset | Reference Address                            |
|-------|-----------------|----------------------------------------------|
| ETH   | WETH            | `0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2` |
| BTC   | WBTC            | `0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599` |

---

## Offchain Prices

### CoinGecko

- **Base URL**: `https://pro-api.coingecko.com/api/v3`
- **Rate Limit**: 450 req/min (of 500 max)
- **Timeout**: 30s, exponential backoff (max 3 retries)
- **Endpoints**: `/simple/price` (current), `/coins/{id}/market_chart/range` (historical)

| Asset  | CoinGecko ID           |
|--------|------------------------|
| DAI    | `dai`                  |
| sDAI   | `savings-dai`          |
| USDC   | `usd-coin`             |
| WETH   | `weth`                 |
| wstETH | `wrapped-steth`        |
| WBTC   | `wrapped-bitcoin`      |
| GNO    | `gnosis`               |
| rETH   | `rocket-pool-eth`      |
| USDT   | `tether`               |
| weETH  | `wrapped-eeth`         |
| cbBTC  | `coinbase-wrapped-btc` |
| sUSDS  | `susds`                |
| USDS   | `usds`                 |
| LBTC   | `lombard-staked-btc`   |
| tBTC   | `tbtc`                 |
| ezETH  | `renzo-restaked-eth`   |
| rsETH  | `kelp-dao-restaked-eth`|
| PYUSD  | `paypal-usd`           |

---

## Multi-Oracle Coverage Summary

Assets covered by multiple onchain oracles:

| Asset  | Chainlink | Chronicle | Redstone |
|--------|:---------:|:---------:|:--------:|
| WETH   | x         | x         | x        |
| WBTC   | x         | x         | x        |
| USDC   | x         | x         | x        |
| USDT   | x         | x         | x        |
| wstETH | x         | x         | x        |
| weETH  | x         |           | x        |
| USDS   | x         | x         |          |
| PYUSD  | x         |           | x        |
| ezETH  | x         |           | x        |
| rsETH  | x         |           | x        |
| LBTC   | x         |           | x        |
| DAI    | x         |           |          |
| cbBTC  | x         |           |          |
| tBTC   | x         |           |          |
| sDAI   | x         |           |          |
| rETH   | x         |           |          |
| sUSDS  |           | x         |          |
