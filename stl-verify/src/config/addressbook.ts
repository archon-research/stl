/**
 * Multi-chain address book for protocol addresses and token configurations
 */

import type { ChainId } from "./chains";

export interface ProtocolAddresses {
    pool?: string;
    poolAddressesProvider?: string;
    oracle?: string;
    [key: string]: string | undefined;
}

export interface TokenInfo {
    symbol: string;
    address: string;
    decimals: number;
}

export interface ChainProtocols {
    sparklend?: ProtocolAddresses;
    aave?: ProtocolAddresses;
    grove?: ProtocolAddresses;
}

export interface BlockMarkers {
    poolCreation?: number;
    oracleOperational?: number;
}

export interface ChainAddressBook {
    protocols: ChainProtocols;
    tokens: TokenInfo[];
    blocks: BlockMarkers;
}

/**
 * Multi-chain address book
 */
const addressBooks: Record<ChainId, ChainAddressBook> = {
    ethereum: {
        protocols: {
            sparklend: {
                pool: "0xC13e21B648A5Ee794902342038FF3aDAB66BE987",
                poolAddressesProvider: "0x02C3eA4e34C0cBd694D2adFa2c690EECbC1793eE",
                oracle: "0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD9"
            },
            grove: {
                // Add Grove addresses when available
            }
        },
        tokens: [ // TODO: Move this into SparkLend as it is specific to their usage
            { symbol: "DAI", address: "0x6B175474E89094C44Da98b954EedeAC495271d0F", decimals: 18 },
            { symbol: "sDAI", address: "0x83F20F44975D03b1b09e64809B757c47f942BEeA", decimals: 18 },
            { symbol: "USDC", address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", decimals: 6 },
            { symbol: "WETH", address: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", decimals: 18 },
            { symbol: "wstETH", address: "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", decimals: 18 },
            { symbol: "WBTC", address: "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", decimals: 8 },
            { symbol: "GNO", address: "0x6810e776880C02933D47DB1b9fc05908e5386b96", decimals: 18 },
            { symbol: "rETH", address: "0xae78736Cd615f374D3085123A210448E74Fc6393", decimals: 18 },
            { symbol: "USDT", address: "0xdAC17F958D2ee523a2206206994597C13D831ec7", decimals: 6 },
            { symbol: "weETH", address: "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee", decimals: 18 },
            { symbol: "cbBTC", address: "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf", decimals: 8 },
            { symbol: "sUSDS", address: "0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD", decimals: 18 },
            { symbol: "USDS", address: "0xdC035D45d973E3EC169d2276DDab16f1e407384F", decimals: 18 },
            { symbol: "LBTC", address: "0x8236a87084f8B84306f72007F36F2618A5634494", decimals: 8 },
            { symbol: "tBTC", address: "0x18084fbA666a33d37592fA2633fD49a74DD93a88", decimals: 18 },
            { symbol: "ezETH", address: "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110", decimals: 18 },
            { symbol: "rsETH", address: "0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7", decimals: 18 },
            { symbol: "PYUSD", address: "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8", decimals: 6 }
        ],
        blocks: {
            poolCreation: 16776401, // TODO: Move this to a protocol
            oracleOperational: 16776437
        }
    },

    base: {
        protocols: {
        },
        tokens: [
        ],
        blocks: {
            // Add relevant block markers when available
        }
    },

    arbitrum: {
        protocols: {
        },
        tokens: [
        ],
        blocks: {
        }
    },

    optimism: {
        protocols: {
        },
        tokens: [
        ],
        blocks: {
        }
    },
    gnosis: {
        protocols: {
        },
        tokens: [
        ],
        blocks: {
        }
    }
};

/**
 * Get address book for a specific chain
 */
export function getAddressBook(chainId: ChainId): ChainAddressBook {
    return addressBooks[chainId];
}

/**
 * Get protocol addresses for a specific chain and protocol
 */
export function getProtocolAddresses(
    chainId: ChainId,
    protocol: keyof ChainProtocols
): ProtocolAddresses | undefined {
    return addressBooks[chainId]?.protocols[protocol];
}

/**
 * Get tokens for a specific chain
 */
export function getTokens(chainId: ChainId): TokenInfo[] {
    return addressBooks[chainId]?.tokens ?? [];
}

/**
 * Create a token lookup map by address (lowercase) for a chain
 */
export function createTokenByAddressMap(chainId: ChainId): Map<string, TokenInfo> {
    const tokens = getTokens(chainId);
    return new Map(tokens.map(token => [token.address.toLowerCase(), token]));
}

/**
 * Get block markers for a specific chain
 */
export function getBlockMarkers(chainId: ChainId): BlockMarkers {
    return addressBooks[chainId]?.blocks ?? {};
}
