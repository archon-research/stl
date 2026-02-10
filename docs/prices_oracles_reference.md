# Prices & Oracles Reference

> ⚠️ **IMPORTANT:** CHAINLINK/PYTH/REDSTONE/CHRONICLE oracle addresses have not had their pairs properly documented (some are ASSET/USD while others are ASSET/USDC). This needs to be fixed.

> ⚠️ **Note:** ABIs for relevant CHAINLINK/PYTH/REDSTONE/CHRONICLE oracles should be added.

## Overview

This document provides a comprehensive reference for pricing assets across various protocols and networks. Assets may be assigned **multiple price sources** to enable different use cases:

- **Priced Via**: Suggested primary pricing method (often a lending protocol oracle like Aave, Morpho, Sparklend)
- **Canonical1 & Canonical2**: Two suggested canonical price sources (on-chain oracles, on-chain methods, or off-chain APIs)
- **Alternative**: An alternative pricing method (if appropriate)
- **CoinGecko ID**: CoinGecko identifier for API-based pricing (where available)

### Why Multiple Price Sources?

1. **Protocol-Native Valuation**: Value lending protocol allocations and backings in the same terms as the protocol itself
2. **Sanity Checks**: Use canonical or alternative sources to verify protocol oracle prices
3. **Alerting**: Trigger alerts when different price sources diverge significantly
4. **Fallbacks**: Use alternative sources when primary sources are unavailable

### Special Value Notation

- **`tbd`** and **`unknown`**: Emphasized throughout - these require research and completion
- `n/a`: Preserved as-is - indicates not applicable

---

## Table of Contents

1. [**Held Addresses**](#held-addresses) - Contract addresses where Star allocations are held
2. [**Oracle Types**](#oracle-types) - Explanation of each oracle/pricing method type
3. [**Core Assets**](#Core-assets) - Core assets with canonical pricing
4. [**Backing Assets**](#backing-assets) - Assets that underlie lending allocations, organized by asset pool
5. [**Allocations**](#allocations) - All current allocations for Spark, Grove, and Obex (under $1M value)

---

## Held Addresses

Contract addresses where Star allocations are held across different blockchains. Each Star may have:
- **ALM**: Asset Liability Management contract
- **PSM**: Peg Stability Module
- **EOA**: Externally Owned Account
- **Anchorage**: Special Anchorage Escrow contract (requires separate monitoring)

```python
HELD_ADDRESSES = {
    "spark": {
        "ethereum": {
            "alm": "0x1601843c5E9bC251A3272907010AFa41Fa18347E",
            "psm": "0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341",
            "anchorage": "0x49506C3Aa028693458d6eE816b2EC28522946872"
        },
        "base": {
            "alm": "0x2917956eFF0B5eaF030abDB4EF4296DF775009cA",
            "psm": "0x1601843c5E9bC251A3272907010AFa41Fa18347E"
        },
        "optimism": {
            "alm": "0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB",
            "psm": "0xe0F9978b907853F354d79188A3dEfbD41978af62"
        },
        "unichain": {
            "alm": "0x345E368fcCd62266B3f5F37C9a131FD1c39f5869",
            "psm": "0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f"
        },
        "arbitrum": {
            "alm": "0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709",
            "psm": "0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266"
        },
        "avalanche": {
            "alm": "0xecE6B0E8a54c2f44e066fBb9234e7157B15b7FeC"
        },
        "plume": {
            "alm": "0x1DB91ad50446a671e2231f77e00948E68876F812"
        },
        "monad": {
            "anchorage": "0x94B398ACb2fcE988871218221EA6a4a2b26CcCbC"
        }
    },
    "grove": {
        "ethereum": {
            "alm": "0x491EDFB0B8b608044e227225C715981a30F3A44E",
            "anchorage": "0x94B398ACb2fcE988871218221EA6a4a2b26CcCbC"
        },
        "avalanche": {
            "alm": "0x7107DD8F56642327945294a18A4280C78e153644"
        },
        "base": {
            "alm": "0x9B746dBC5269e1DF6e4193Bcb441C0FbBF1CeCEe"
        },
        "monad": {
            "eoa": "0x94B398ACb2fcE988871218221EA6a4a2b26CcCbC"
        }
    },
    "obex": {
        "ethereum": {
            "alm": "0xb6dD7ae22C9922AFEe0642f9Ac13e58633f715A2"
        }
    }
}
```

---

## Oracle Types

### Core
Reference the **Core Assets** section below to determine pricing. These are core assets with well-established on-chain oracles.

### Lending Protocol Native Oracles
Assets lent or borrowed on lending protocols (Aave, Morpho, Sparklend, etc.) have an oracle assigned to them at any given block. Fetch prices via the protocol's contracts using the oracle assigned to each reserve.

### Chainlink
Push oracle - each price feed has a dedicated smart contract address. Query the contract directly for the latest price.

### Chronicle
Push oracle - each price feed has a dedicated smart contract address. Query the contract directly for the latest price.

### Redstone
Push oracle - each price feed has a dedicated smart contract address. Query the contract directly for the latest price.

### Chainlink Pull
Pull oracle requiring a paid blockchain transaction to fetch prices. Unlike push oracles, **all prices are fetched from a single contract using an ID** (e.g., `chainlinkId:0x0003...`). Avoid using these oracles where possible due to transaction fees.

### Pyth
Pull oracle requiring a paid blockchain transaction to fetch prices. **All prices are fetched from a single contract using an ID** (e.g., `pythId:0xe616...`). Avoid using these oracles where possible due to transaction fees.

### External APIs
Off-chain data sources requiring HTTP requests. Currently, possible values are:
- **Centrifuge API**: Real-time prices for tokenized real-world assets (see `rwas_spec.md` for details)
- **Maple API**: Price data for Maple Finance assets (see `maple_spec.md` for details)
- **Securitize API**: Pricing for Securitize-issued tokens (see `rwas_spec.md` for details)

---

## Core Assets

Core assets used as underlying or backing assets across multiple protocols. **We will use the on-chain Ethereum price** for these assets. Take special note when these prices are referenced in situations that do not reflect Ethereum mainnet pricing.

```python
Core_ASSETS = {
    "USDS": {
        "network": "ethereum",
        "address": "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
        "priced_via": "canonical1",
        "canonical1": {
            "type": "chronicle",
            "address": "0x74661a9ea74fD04975c6eBc6B155Abf8f885636c"
        },
        "canonical2": {
            "type": "chainlink",
            "address": "0xfF30586cD0F29eD462364C7e81375FC0C71219b1"
        },
        "coingecko_id": "usds"
    },
    "DAI": {
        "network": "ethereum",
        "address": "0x6b175474e89094c44da98b954eedeac495271d0f",
        "priced_via": **"unknown"**,
        "canonical1": {
            "type": **"unknown"**
        },
        "canonical2": {
            "type": "chainlink",
            "address": "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9"
        },
        "coingecko_id": "dai"
    },
    "USDC": {
        "network": "ethereum",
        "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "priced_via": "canonical1",
        "canonical1": {
            "type": "chronicle",
            "address": "0xCe701340261a3dc3541C5f8A6d2bE689381C8fCC"
        },
        "canonical2": {
            "type": "chainlink",
            "address": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
        },
        "coingecko_id": "usd-coin"
    },
    "USDT": {
        "network": "ethereum",
        "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "priced_via": "canonical1",
        "canonical1": {
            "type": "chronicle",
            "address": "0x7084a627a22b2de99E18733DC5aAF40993FA405C"
        },
        "canonical2": {
            "type": "chainlink",
            "address": "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D"
        },
        "coingecko_id": "tether"
    },
    "BTC/WBTC": {
        "network": "ethereum",
        "address": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        "priced_via": "canonical1",
        "canonical1": {
            "type": "chronicle",
            "address": "0x286204401e0C1E63043E95a8DE93236B735d4BF2"
        },
        "canonical2": {
            "type": "chainlink",
            "address": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"
        },
        "coingecko_id": "bitcoin"
    },
    "ETH/WETH": {
        "network": "ethereum",
        "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "priced_via": "canonical1",
        "canonical1": {
            "type": "chronicle",
            "address": "0xb074EEE1F1e66650DA49A4d96e255c8337A272a9"
        },
        "canonical2": {
            "type": "chainlink",
            "address": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
        },
        "coingecko_id": "ethereum"
    },
    "wstETH": {
        "network": "ethereum",
        "address": "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
        "priced_via": "canonical1",
        "canonical1": {
            "type": "chronicle",
            "address": "0xA770582353b573CbfdCC948751750EeB3Ccf23CF"
        },
        "canonical2": {
            "type": "pyth",
            "id": "pythId:0x6df640f3b8963d8f8358f791f352b8364513f6ab1cca5ed3f1f7b5448980e784"
        },
        "coingecko_id": "wrapped-steth"
    }
}
```

---

## Backing Assets

Assets that are not necessarily held by a Star, but are the underlying for a lending allocation. These may need to be priced to determine the composition of lending allocations. The **Asset Pool** value matches a Star allocation to its complete set of backing assets.

**Note:** Contract addresses for backing assets were not included in the source data and need to be added.

```python
BACKING_ASSETS = {
    "aave1": {
        "ethereum": [
            {"symbol": "1INCH", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "1inch"},
            {"symbol": "AAVE", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "aave"},
            {"symbol": "all Pendle sUSDe (varies)", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "n/a"},
            {"symbol": "all Pendle USDe (varies)", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "n/a"},
            {"symbol": "BAL", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "balancer"},
            {"symbol": "cbBTC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "coinbase-wrapped-btc"},
            {"symbol": "cbETH", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "coinbase-wrapped-staked-eth"},
            {"symbol": "CRV", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "curve-dao-token"},
            {"symbol": "DAI", "priced_via": "aave oracle", "canonical": **"unknown"**, "coingecko_id": "dai"},
            {"symbol": "eBTC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "ether-fi-staked-btc"},
            {"symbol": "ENS", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "ethereum-name-service"},
            {"symbol": "ETH", "priced_via": "aave oracle", "canonical": "core", "coingecko_id": "ethereum"},
            {"symbol": "eUSDe", "priced_via": "aave oracle", "canonical": **"tbd"**},
            {"symbol": "ETHx", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "stader-ethx"},
            {"symbol": "EURC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "euro-coin"},
            {"symbol": "ezETH", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "renzo-restaked-eth"},
            {"symbol": "FBTC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "ignition-fbtc"},
            {"symbol": "FRAX", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "frax"},
            {"symbol": "LBTC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "lombard-staked-btc"},
            {"symbol": "LDO", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "lido-dao"},
            {"symbol": "LINK", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "chainlink"},
            {"symbol": "LUSD", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "liquity-usd"},
            {"symbol": "osETH", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "stakewise-v3-oseth"},
            {"symbol": "PYUSD", "priced_via": "aave oracle", "canonical": **"unknown"**, "coingecko_id": "paypal-usd"},
            {"symbol": "rETH", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "rocket-pool-eth"},
            {"symbol": "rsETH", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "kelp-dao-restaked-eth"},
            {"symbol": "sDAI", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "savings-dai"},
            {"symbol": "SNX", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "havven"},
            {"symbol": "sUSDe", "priced_via": "aave oracle", "canonical": "chainlink", "address": "0xFF3BC18cCBd5999CE63E788A1c250a88626aD099", "coingecko_id": "ethena-staked-usde"},
            {"symbol": "syrupUSDC", "priced_via": "aave oracle", "canonical": "erc4626 totalAssets", "coingecko_id": "syrupusdc"},
            {"symbol": "tBTC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "tbtc"},
            {"symbol": "tETH", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "treehouse-eth"},
            {"symbol": "UNI", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "uniswap"},
            {"symbol": "USDC", "priced_via": "aave oracle", "canonical": "core", "coingecko_id": "usd-coin"},
            {"symbol": "USDe", "priced_via": "aave oracle", "canonical": "chainlink", "address": "0xa569d910839Ae8865Da8F8e70FfFb0cBA869F961", "coingecko_id": "ethena-usde"},
            {"symbol": "USDS", "priced_via": "aave oracle", "canonical": "core", "coingecko_id": "usds"},
            {"symbol": "USDT", "priced_via": "aave oracle", "canonical": "core", "coingecko_id": "tether"},
            {"symbol": "WBTC", "priced_via": "aave oracle", "canonical": "core", "coingecko_id": "wrapped-bitcoin"},
            {"symbol": "weETH", "priced_via": "aave oracle", "canonical": "chronicle", "address": "0x6a906372cA06523bA7FeaeDab18Ab8B665CaeD71", "coingecko_id": "wrapped-eeth"},
            {"symbol": "WETH", "priced_via": "aave oracle", "canonical": "core", "coingecko_id": "ethereum"},
            {"symbol": "wstETH", "priced_via": "aave oracle", "canonical": "core", "coingecko_id": "wrapped-steth"},
            {"symbol": "XAUt", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "tether-gold"}
        ]
    },
    "aave2": {
        "ethereum": [
            {"symbol": "JAAA", "priced_via": "aave oracle", "canonical": "chronicle", "address": "0x02cf8C9fBa24d79886dAc40cb620f0930C6E8eC0", "coingecko_id": "janus-henderson-anemoy-aaa-clo-fund"},
            {"symbol": "JTRSY", "priced_via": "aave oracle", "canonical": "chronicle", "address": "0x59ef4BE3eDDF0270c4878b7B945bbeE13fb33d0D", "coingecko_id": "janus-henderson-anemoy-treasury-fund"},
            {"symbol": "USCC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "superstate-uscc"},
            {"symbol": "USTB", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "superstate-short-duration-us-government-securities-fund-ustb"},
            {"symbol": "USYC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "hashnote-usyc"},
            {"symbol": "VBILL", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "vaneck-treasury-fund"}
        ]
    },
    "aave3": {
        "avalanche": [
            {"symbol": "AAVE.e", "priced_via": "aave oracle", "canonical": **"tbd"**},
            {"symbol": "AUSD", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "agora-dollar"},
            {"symbol": "AVAX", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "avalanche-2"},
            {"symbol": "BTC.b", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "bitcoin-avalanche-bridged-btc-b"},
            {"symbol": "DAI.e", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "avalanche-bridged-dai-avalanche"},
            {"symbol": "EURC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "euro-coin"},
            {"symbol": "FRAX", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "frax"},
            {"symbol": "LINK.e", "priced_via": "aave oracle", "canonical": **"tbd"**},
            {"symbol": "sAVAX", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "benqi-liquid-staked-avax"},
            {"symbol": "sUSDe", "priced_via": "aave oracle", "canonical": "chainlink", "address": "0xFF3BC18cCBd5999CE63E788A1c250a88626aD099", "coingecko_id": "ethena-staked-usde"},
            {"symbol": "USDC", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "usd-coin"},
            {"symbol": "USDe", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "ethena-usde"},
            {"symbol": "USDt", "priced_via": "aave oracle", "canonical": **"tbd"**},
            {"symbol": "WAVAX", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "wrapped-avax"},
            {"symbol": "WETH.e", "priced_via": "aave oracle", "canonical": **"tbd"**, "coingecko_id": "avalanche-bridged-weth-avalanche"},
            {"symbol": "wrsETH", "priced_via": "aave oracle", "canonical": **"tbd"**}
        ]
    },
    "curve1": {
        "ethereum": [
            {"symbol": "USDT", "priced_via": "canonical", "canonical": **"tbd"**, "coingecko_id": "tether"}
        ]
    },
    "curve2": {
        "ethereum": [
            {"symbol": "PYUSD", "priced_via": "canonical", "canonical": **"unknown"**, "coingecko_id": "paypal-usd"}
        ]
    },
    "maple1": {
        "bitcoin": [
            {"symbol": "BTC", "priced_via": "core", "canonical": **"tbd"**, "coingecko_id": "bitcoin"}
        ],
        "ethereum": [
            {"symbol": "LBTC", "priced_via": **"tbd"**, "canonical": **"tbd"**, "coingecko_id": "lombard-staked-btc"},
            {"symbol": "weETH", "priced_via": **"tbd"**, "canonical": "chronicle", "address": "0x6a906372cA06523bA7FeaeDab18Ab8B665CaeD71", "coingecko_id": "wrapped-eeth"}
        ],
        "OTC": [
            {"symbol": "HYPE", "priced_via": **"tbd"**, "canonical": **"tbd"**, "coingecko_id": "hyperliquid"},
            {"symbol": "XRP", "priced_via": **"tbd"**, "canonical": **"tbd"**, "coingecko_id": "ripple"}
        ]
    },
    "morpho1": {
        "ethereum": [
            {"symbol": "ACRDX", "priced_via": "morpho oracle", "canonical": **"tbd"**, "coingecko_id": "anemoy-tokenized-apollo-diversified-credit-fund"},
            {"symbol": "cbBTC", "priced_via": "morpho oracle", "canonical": **"tbd"**, "coingecko_id": "coinbase-wrapped-btc"},
            {"symbol": "LBTC", "priced_via": "morpho oracle", "canonical": "core", "coingecko_id": "lombard-staked-btc"},
            {"symbol": "Pendle sUSDe (varies)", "priced_via": "morpho oracle", "canonical": **"tbd"**, "coingecko_id": "n/a"},
            {"symbol": "Pendle USDe (varies)", "priced_via": "morpho oracle", "canonical": **"tbd"**, "coingecko_id": "n/a"},
            {"symbol": "syrupUSDC", "priced_via": "morpho oracle", "canonical": "erc4626 totalAssets", "coingecko_id": "syrupusdc"},
            {"symbol": "WBTC", "priced_via": "morpho oracle", "canonical": **"tbd"**, "coingecko_id": "wrapped-bitcoin"},
            {"symbol": "wstETH", "priced_via": "morpho oracle", "canonical": "core", "coingecko_id": "wrapped-steth"}
        ]
    },
    "morpho2": {
        "base": [
            {"symbol": "cbBTC", "priced_via": "morpho oracle", "canonical": **"tbd"**, "coingecko_id": "coinbase-wrapped-btc"},
            {"symbol": "cbETH", "priced_via": "morpho oracle", "canonical": **"tbd"**, "coingecko_id": "coinbase-wrapped-staked-eth"},
            {"symbol": "WETH", "priced_via": "morpho oracle", "canonical": "core", "coingecko_id": "ethereum"},
            {"symbol": "wstETH", "priced_via": "morpho oracle", "canonical": "core", "coingecko_id": "wrapped-steth"}
        ]
    },
    "sparklend": {
        "ethereum": [
            {"symbol": "cbBTC", "priced_via": "sparklend oracle", "canonical": **"tbd"**, "coingecko_id": "coinbase-wrapped-btc"},
            {"symbol": "ezETH", "priced_via": "sparklend oracle", "canonical": "chronicle", "coingecko_id": "renzo-restaked-eth"},
            {"symbol": "LBTC", "priced_via": "sparklend oracle", "canonical": **"tbd"**, "coingecko_id": "lombard-staked-btc"},
            {"symbol": "rETH", "priced_via": "sparklend oracle", "canonical": **"tbd"**, "coingecko_id": "rocket-pool-eth"},
            {"symbol": "rsETH", "priced_via": "sparklend oracle", "canonical": **"tbd"**, "coingecko_id": "kelp-dao-restaked-eth"},
            {"symbol": "sDAI", "priced_via": "sparklend oracle", "canonical": **"tbd"**, "coingecko_id": "savings-dai"},
            {"symbol": "sUSDS", "priced_via": "sparklend oracle", "canonical": "chronicle", "address": "0x496470F4835186bF118545Bd76889F123D608E84", "coingecko_id": "susds"},
            {"symbol": "tBTC", "priced_via": "sparklend oracle", "canonical": **"tbd"**, "coingecko_id": "tbtc"},
            {"symbol": "USDT", "priced_via": "sparklend oracle", "canonical": "core", "coingecko_id": "tether"},
            {"symbol": "WBTC", "priced_via": "sparklend oracle", "canonical": **"tbd"**, "coingecko_id": "wrapped-bitcoin"},
            {"symbol": "weETH", "priced_via": "sparklend oracle", "canonical": "chronicle", "address": "0x6a906372cA06523bA7FeaeDab18Ab8B665CaeD71", "coingecko_id": "wrapped-eeth"},
            {"symbol": "WETH", "priced_via": "sparklend oracle", "canonical": "core", "coingecko_id": "ethereum"},
            {"symbol": "wstETH", "priced_via": "sparklend oracle", "canonical": **"tbd"**, "coingecko_id": "wrapped-steth"}
        ]
    },
    "uniswap1": {
        "ethereum": [
            {"symbol": "AUSD", "priced_via": **"tbd"**, "canonical": **"tbd"**, "coingecko_id": "agora-dollar"}
        ]
    }
}
```

---

## Allocations

All current allocations for Stars **under $1M in value**.

### Reference Notes
- `held_address_ref`: References the **Held Addresses** section above
- `backing_ref`: References the **Backing Assets** section above
- `priced_via`: Can be a standalone value (e.g., "sparklend oracle") OR a reference to `canonical1`, `canonical2`, or `alternative`
- **Bold** values indicate **`tbd`** or **`unknown`** - requires research
- `n/a` preserved as-is
- Blank values omitted

```python
ALLOCATIONS = {
    "spark": {
        "aave": {
            "avalanche": [
                {
                    "symbol": "aAvaUSDC",
                    "name": "Aave Avalanche USDC",
                    "token_address": "0x625e7708f30ca75bfd92586e17077590c60eb4cd",
                    "underlying_address": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
                    "held_address_ref": "spark.avalanche.alm",
                    "backing_ref": "aave3",
                    "priced_via": "aave oracle",
                    "canonical1": {"type": "core"},
                    "coingecko_id": "n/a"
                }
            ]
        },
        "anchorage": {
            "ethereum": [
                {
                    "symbol": "Anchorage",
                    "name": "Anchorage",
                    "token_address": "none",
                    "held_address_ref": "spark.ethereum.anchorage",
                    "priced_via": **"unknown"**,
                    "canonical1": {"type": "none"},
                    "canonical2": {"type": "none"},
                    "coingecko_id": "none"
                }
            ]
        },
        "arkis": {
            "ethereum": [
                {
                    "symbol": "sparkPrimeUSDC1",
                    "name": "Spark Prime USDC 1",
                    "token_address": "0x38464507E02c983F20428a6E8566693fE9e422a9",
                    "held_address_ref": "spark.ethereum.alm",
                    "priced_via": "alternative",
                    "canonical2": {"type": "none"},
                    "alternative": "erc4626 totalAssets",
                    "coingecko_id": "none"
                }
            ]
        },
        "curve": {
            "ethereum": [
                {
                    "symbol": "PYUSDUSDS",
                    "name": "Spark.fi PYUSD Reserve",
                    "token_address": "0xA632D59b9B804a956BfaA9b48Af3A1b74808FC1f",
                    "held_address_ref": "spark.ethereum.alm",
                    "priced_via": "price underlying cores",
                    "canonical1": {"type": "n/a"},
                    "canonical2": {"type": "n/a"},
                    "coingecko_id": "n/a"
                },
                {
                    "symbol": "sUSDSUSDT",
                    "name": "Spark.fi USDT Reserve",
                    "token_address": "0x00836Fe54625BE242BcFA286207795405ca4fD10",
                    "held_address_ref": "spark.ethereum.alm",
                    "backing_ref": "curve1",
                    "priced_via": "price underlying cores",
                    "canonical1": {"type": "n/a"},
                    "canonical2": {"type": "n/a"},
                    "coingecko_id": "n/a"
                }
            ]
        },
        "maple": {
            "ethereum": [
                {
                    "symbol": "syrupUSDC",
                    "name": "Syrup USDC",
                    "token_address": "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b",
                    "held_address_ref": "spark.ethereum.alm",
                    "backing_ref": "maple1",
                    "priced_via": "alternative",
                    "canonical1": {"type": "pyth", "id": "pythId:0xe616297dab48626eaacf6d030717b25823b13ae6520b83f4735bf8deec8e2c9a"},
                    "canonical2": {"type": "maple API"},
                    "alternative": "erc4626 totalAssets",
                    "coingecko_id": "syrupusdc"
                }
            ]
        },
        "paypal": {
            "ethereum": [
                {
                    "symbol": "PYUSD",
                    "name": "PayPal USD",
                    "token_address": "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8",
                    "held_address_ref": "spark.ethereum.alm",
                    "priced_via": **"unknown"**,
                    "canonical1": {"type": "chainlink pull", "id": "chainlinkId:0x0003313e5ba57741dd23e2e239730977b3a2a65fcdca08a8609c6e5bac09f88f"},
                    "canonical2": {"type": "pyth", "id": "pythId:0xc1da1b73d7f01e7ddd54b3766cf7fcd644395ad14f70aa706ec5384c59e76692"},
                    "coingecko_id": "paypal-usd"
                }
            ]
        },
        "sparklend": {
            "ethereum": [
                {
                    "symbol": "spDAI",
                    "name": "Spark DAI",
                    "token_address": "0x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B",
                    "underlying_address": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                    "held_address_ref": "spark.ethereum.alm",
                    "backing_ref": "sparklend",
                    "priced_via": "sparklend oracle",
                    "canonical1": {"type": "core"},
                    "coingecko_id": "n/a"
                },
                {
                    "symbol": "spPYUSD",
                    "name": "Spark PYUSD",
                    "token_address": "0x779224df1c756b4EDD899854F32a53E8c2B2ce5d",
                    "underlying_address": "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8",
                    "held_address_ref": "spark.ethereum.alm",
                    "backing_ref": "sparklend",
                    "priced_via": "sparklend oracle",
                    "canonical1": {"type": "chainlink pull", "id": "chainlinkId:0x0003313e5ba57741dd23e2e239730977b3a2a65fcdca08a8609c6e5bac09f88f"},
                    "canonical2": {"type": "pyth", "id": "pythId:0xc1da1b73d7f01e7ddd54b3766cf7fcd644395ad14f70aa706ec5384c59e76692"},
                    "coingecko_id": "n/a"
                },
                {
                    "symbol": "spUSDS",
                    "name": "Spark USDS",
                    "token_address": "0xC02aB1A5eaA8d1B114EF786D9bde108cD4364359",
                    "underlying_address": "0xdC035D45d973E3EC169d2276DDab16f1e407384F",
                    "held_address_ref": "spark.ethereum.alm",
                    "backing_ref": "sparklend",
                    "priced_via": "sparklend oracle",
                    "canonical1": {"type": "core"},
                    "coingecko_id": "n/a"
                },
                {
                    "symbol": "spUSDC",
                    "name": "Spark USDC",
                    "token_address": "0x377C3bd93f2a2984E1E7bE6A5C22c525eD4A4815",
                    "underlying_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                    "held_address_ref": "spark.ethereum.alm",
                    "backing_ref": "sparklend",
                    "priced_via": "sparklend oracle",
                    "canonical1": {"type": "core"},
                    "coingecko_id": "n/a"
                },
                {
                    "symbol": "spUSDT",
                    "name": "Spark USDT",
                    "token_address": "0xe7dF13b8e3d6740fe17CBE928C7334243d86c92f",
                    "underlying_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                    "held_address_ref": "spark.ethereum.alm",
                    "backing_ref": "sparklend",
                    "priced_via": "sparklend oracle",
                    "canonical1": {"type": "core"},
                    "coingecko_id": "n/a"
                }
            ]
        }
    },
    "grove": {
        "aave": {
            "ethereum": [
                {
                    "symbol": "aEthRLUSD",
                    "name": "Aave Ethereum RLUSD",
                    "token_address": "0xFa82580c16A31D0c1bC632A36F82e83EfEF3Eec0",
                    "underlying_address": "0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD",
                    "held_address_ref": "grove.ethereum.alm",
                    "backing_ref": "aave1",
                    "priced_via": "aave oracle",
                    "canonical1": {"type": "chainlink", "address": "0x26C46B7aD0012cA71F2298ada567dC9Af14E7f2A"},
                    "coingecko_id": "n/a"
                },
                {
                    "symbol": "aHorRwaRLUSD",
                    "name": "Aave Horizon RWA RLUSD",
                    "token_address": "0xE3190143Eb552456F88464662f0c0C4aC67A77eB",
                    "underlying_address": "0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD",
                    "held_address_ref": "grove.ethereum.alm",
                    "backing_ref": "aave2",
                    "priced_via": "aave oracle",
                    "canonical1": {"type": "chainlink", "address": "0x26C46B7aD0012cA71F2298ada567dC9Af14E7f2A"},
                    "coingecko_id": "n/a"
                },
                {
                    "symbol": "aHorRwaUSDC",
                    "name": "Aave Horizon RWA USDC",
                    "token_address": "0x68215B6533c47ff9f7125aC95adf00fE4a62f79e",
                    "underlying_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                    "held_address_ref": "grove.ethereum.alm",
                    "backing_ref": "aave2",
                    "priced_via": "aave oracle",
                    "canonical1": {"type": "core"},
                    "coingecko_id": "n/a"
                }
            ]
        },
        "blackrock": {
            "ethereum": [
                {
                    "symbol": "BUIDL-I",
                    "name": "BlackRock USD Institutional Digital Liquidity Fund - I Class",
                    "token_address": "0x6a9DA2D710BB9B700acde7Cb81F10F1fF8C89041",
                    "held_address_ref": "grove.ethereum.alm",
                    "priced_via": "canonical1",
                    "canonical1": {"type": "redstone", "address": "0xb9BD795BB71012c0F3cd1D9c9A4c686F2d3524A4"},
                    "canonical2": {"type": "securitize API"},
                    "coingecko_id": "blackrock-usd-institutional-digital-liquidity-fund-i-class"
                }
            ]
        },
        "centrifuge": {
            "avalanche": [
                {
                    "symbol": "JAAA",
                    "name": "Janus Henderson Anemoy AAA CLO Fund Token",
                    "token_address": "0x58F93d6b1EF2F44eC379Cb975657C132CBeD3B6b",
                    "held_address_ref": "grove.avalanche.alm",
                    "priced_via": "canonical1",
                    "canonical1": {"type": "chronicle", "address": "0x02cf8C9fBa24d79886dAc40cb620f0930C6E8eC0"},
                    "canonical2": {"type": "centrifuge API"},
                    "alternative": "erc4626 totalAssets from vault contract (not token contract)",
                    "coingecko_id": "janus-henderson-anemoy-aaa-clo-fund"
                }
            ],
            "ethereum": [
                {
                    "symbol": "JAAA",
                    "name": "Janus Henderson Anemoy AAA CLO Fund Token",
                    "token_address": "0x5a0F93D040De44e78F251b03c43be9CF317Dcf64",
                    "held_address_ref": "grove.ethereum.alm",
                    "priced_via": "canonical1",
                    "canonical1": {"type": "chronicle"},
                    "canonical2": {"type": "centrifuge API"},
                    "alternative": "erc4626 totalAssets from vault contract (not token contract)",
                    "coingecko_id": "janus-henderson-anemoy-aaa-clo-fund"
                },
                {
                    "symbol": "JTRSY",
                    "name": "Janus Henderson Anemoy Treasury Fund",
                    "token_address": "0x8c213ee79581Ff4984583C6a801e5263418C4b86",
                    "held_address_ref": "grove.ethereum.alm",
                    "priced_via": "canonical1",
                    "canonical1": {"type": "chronicle", "address": "0x59ef4BE3eDDF0270c4878b7B945bbeE13fb33d0D"},
                    "canonical2": {"type": "centrifuge API"},
                    "alternative": "erc4626 totalAssets from vault contract (not token contract)",
                    "coingecko_id": "janus-henderson-anemoy-treasury-fund"
                }
            ],
            "plume": [
                {
                    "symbol": "ACRDX",
                    "name": "Anemoy Tokenized Apollo Diversified Credit Fund Token",
                    "token_address": "0x9477724Bb54AD5417de8Baff29e59DF3fB4DA74f",
                    "held_address_ref": "grove.plume.alm",
                    "priced_via": "canonical1",
                    "canonical1": {"type": "chronicle", "address": "0x51cC9463788b870D1e9Bacd111a9bbB2C9820c7e"},
                    "canonical2": {"type": "centrifuge API"},
                    "alternative": "erc4626 totalAssets from vault contract (not token contract)",
                    "coingecko_id": "anemoy-tokenized-apollo-diversified-credit-fund"
                }
            ]
        },
        "curve": {
            "ethereum": [
                {
                    "symbol": "AUSDUSDC",
                    "name": "CURVE LP AUSD/USDC",
                    "token_address": "0xe79c1c7e24755574438a26d5e062ad2626c04662",
                    "held_address_ref": "grove.ethereum.alm",
                    "priced_via": "price underlying backing",
                    "canonical1": {"type": "n/a"},
                    "coingecko_id": "n/a"
                }
            ]
        },
        "inx": {
            "avalanche": [
                {
                    "symbol": "GACLO-1",
                    "name": "Galaxy Arch CLO Token",
                    "token_address": "0x2C0aDFF8e114f3cA106051144353aC703D24B901",
                    "held_address_ref": "grove.avalanche.alm",
                    "priced_via": **"unknown"**,
                    "canonical1": {"type": "none"},
                    "coingecko_id": "none"
                }
            ]
        },
        "morpho": {
            "base": [
                {
                    "symbol": "grove-bbqUSDC",
                    "name": "Grove x Steakhouse USDC High Yield",
                    "token_address": "0xBeEf2d50B428675a1921bC6bBF4bfb9D8cF1461A",
                    "held_address_ref": "grove.base.alm",
                    "backing_ref": "morpho2",
                    "priced_via": "morpho oracle",
                    "canonical1": {"type": "none"},
                    "coingecko_id": "none"
                }
            ],
            "ethereum": [
                {
                    "symbol": "grove-bbqUSDC-V2",
                    "name": "Grove x Steakhouse USDC",
                    "token_address": "0xBeefF08dF54897e7544aB01d0e86f013DA354111",
                    "held_address_ref": "grove.ethereum.alm",
                    "backing_ref": "morpho1",
                    "priced_via": "morpho oracle",
                    "canonical1": {"type": "none"},
                    "coingecko_id": "none"
                }
            ],
            "monad": [
                {
                    "symbol": "grove-bbqAUSD",
                    "name": "Grove x Steakhouse High Yield AUSD",
                    "token_address": "0x32841A8511D5c2c5b253f45668780B99139e476D",
                    "held_address_ref": "grove.monad.eoa",
                    "priced_via": "morpho oracle",
                    "canonical1": {"type": "none"},
                    "coingecko_id": "none"
                }
            ]
        },
        "securitize": {
            "ethereum": [
                {
                    "symbol": "STAC",
                    "name": "Securitize Tokenized AAA CLO Fund",
                    "token_address": "0x51C2d74017390CbBd30550179A16A1c28F7210fc",
                    "held_address_ref": "grove.ethereum.alm",
                    "priced_via": "canonical1",
                    "canonical1": {"type": "chronicle", "address": "0x9D77E4cA90E25114AFb24dF908f5918f572D958B"},
                    "canonical2": {"type": "redstone", "address": "0xEdC6287D3D41b322AF600317628D7E226DD3add4"},
                    "coingecko_id": "securitize-tokenized-aaa-clo-fund"
                }
            ]
        },
        "uniswap": {
            "ethereum": [
                {
                    "symbol": "AUSDUSDC",
                    "name": "Uniswap LP AUSD/USDC",
                    "token_address": "0xbAFeAd7c60Ea473758ED6c6021505E8BBd7e8E5d",
                    "held_address_ref": "grove.ethereum.alm",
                    "backing_ref": "uniswap1",
                    "priced_via": "price underlying backing",
                    "canonical1": {"type": "n/a"},
                    "coingecko_id": "n/a"
                }
            ]
        }
    },
    "obex": {
        "maple": {
            "ethereum": [
                {
                    "symbol": "syrupUSDC",
                    "name": "Syrup USDC",
                    "token_address": "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b",
                    "held_address_ref": "obex.ethereum.alm",
                    "backing_ref": "maple1",
                    "priced_via": "alternative",
                    "canonical1": {"type": "pyth", "id": "pythId:0xe616297dab48626eaacf6d030717b25823b13ae6520b83f4735bf8deec8e2c9a"},
                    "canonical2": {"type": "maple API"},
                    "alternative": "erc4626 totalAssets",
                    "coingecko_id": "syrupusdc"
                }
            ]
        }
    }
}
```

---

## Document End
